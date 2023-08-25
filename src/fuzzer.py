import pandas as pd
import logging
import subprocess
import re
import os
import random
import signal
import time
from tqdm import tqdm
import string
from .config import CONFIG
from .utils import TestCaseColumns

# MAXINT in SQLite is 9223372036854775807
# from data import filters


class Fuzzer():
    MAX_TIME_PER_RUN = 5

    def __init__(self, seed: int, mode) -> None:
        self.test_cases: pd.DataFrame = None
        self.sql_list = []
        self.input = ""
        self.seed = seed

    def timeout_handler(self, signum, frame):
        raise TimeoutError

    def run(self, command: str) -> None:
        subprocess.run(command, shell=True)

    def load(self, path: str) -> pd.DataFrame:
        self.test_cases = pd.read_csv(path)
        
    def load_suite(self, path: str) -> pd.DataFrame:
        all_test = pd.DataFrame()
        test_files = []
        g = os.walk(path)
        for path, _, file_list in g:
            test_files += [os.path.join(path, file) for file in file_list]
        for test_file in tqdm(test_files):
            df = pd.read_csv(test_file)
            df['TESTFILE_PATH'] = test_file
            all_test = pd.concat([all_test, df], ignore_index=True)
        self.test_cases = all_test
        logging.debug(self.test_cases)

    def extract(self, test_case) -> None:
        self.sql_list = self.test_cases['SQL'].tolist()

    def init_db(self, db_name):
        pass
    
    def mutate(self) -> None:
        pass


class SimpleFuzzer(Fuzzer):
    # MAX_INT_POOL = [-9223372036854775808, -1, 0, 1, 9223372036854775807]
    INT_POOL = [-1, 0, 1, 9223372036854775807, -9223372036854775808]
    SEG_POOL = [(-1, 1), (0, 1), (1, 14),  (-0.5, 0.5)]
    OP_POOL = [
        "==", "!=", "<>", "<", "<=", ">", ">=", "=",
    ]

    FUZZING_TAG = ["INT", "NUMERIC", "STR"]

    CONSTANT_REGEX = re.compile(r"(\b\d+(\.\d+)?\b)|('(?:[^']|'')*')")
    OPERATOR_REGEX = re.compile(
        r"(==|!=|<>|<=|>=|=|<|>)")

    def __init__(self, seed, mode) -> None:
        super().__init__(seed, mode)
        self.cmd = []
        self.cli = None
        self.mode = mode
        self.iter_times = tqdm(range(100)) if logging.getLogger(
        ).getEffectiveLevel() == logging.DEBUG else range(3000)

    def setup_summary(self):
        logging.info("Setup Summary")
        logging.info(f"Fuzzer: {self.__class__.__name__}")
        logging.info(f"Seed: {self.seed}")
        logging.info(f"Using DBMS {self.cmd}")
        logging.info(f"Fuzzing Tag: {self.FUZZING_TAG}")
        logging.info(f"Int Pool: {self.INT_POOL}")
        logging.info(f"Seg Pool: {self.SEG_POOL}")
        logging.info(f"Iteration Times: {len(self.iter_times)}")

    def _random_generator(self, tag: str):
        assert tag in self.FUZZING_TAG, f"Tag {tag} not supported"
        if tag == "INT":
            return self._get_random_int
        elif tag == "NUMERIC":
            return self._get_random_numeric
        elif tag == "STR":
            return self._get_random_string
        else:
            return None

    def _get_random_int(self, match) -> str:
        return str(random.choice(self.INT_POOL))

    def _get_random_numeric(self, match) -> str:
        seg = random.choice(self.SEG_POOL)
        return str(random.uniform(seg[0], seg[1]))

    def _get_random_string(self, match) -> str:
        return "'" + ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(random.randint(1, 20))) + "'"

    def _get_random_op(self, match) -> str:
        return random.choice(self.OP_POOL)

    def constant_mutator(self, sql: str, tag) -> str:
        """substite all the constant in the sql with a random new constant

        Args:
            sql (str): sql to be mutated
            tag (str): type of constant to be mutated

        Returns:
            str: sql after mutation
        """
        return re.sub(self.CONSTANT_REGEX, self._random_generator(tag), sql)

    def operator_mutator(self, sql: str) -> str:
        return re.sub(self.OPERATOR_REGEX, self._get_random_op, sql)

    def filter(self, df) -> None:
        return df
    
    def extract(self, test_case) -> None:
        logging.info(f"Extracting test case {test_case}")
        df: pd.DataFrame = self.test_cases
        df = df[df['TESTFILE_PATH'] == test_case]
        df = self.filter(df)
        self.sql_list = [str(sql).removesuffix(
            ';') + ';' for sql in df['SQL'].tolist()]
        logging.info(f"Length of sql list: {len(self.sql_list)}")
    
    def mutate(self, iter=False, non = False) -> None:
        if non:
            self.input = "\n".join(self.sql_list)
            return
        # replace every number in the sql with a random new number
        if iter:
            self.sql_list = [self.constant_mutator(
                str(sql), random.choice(self.FUZZING_TAG)) for sql in self.sql_list]
            self.input = "\n".join(self.sql_list)
        else:
            self.input = "\n".join([self.constant_mutator(
                str(sql), random.choice(self.FUZZING_TAG)) for sql in self.sql_list])

        self.input = self.operator_mutator(self.input)

    def crash_signal(self, err):
        logging.debug("err: " + err)
        return self.cli.returncode < 0

    def run(self) -> int:
        status = True
        logging.debug(self.cmd)
        self.cli = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, encoding='utf-8', universal_newlines=True, bufsize=2 << 15)
        out, err = self.cli.communicate(input=self.input)
        logging.debug("input: ")
        logging.debug(self.input)
        logging.debug("output: ")
        logging.debug(out)
        logging.debug("return code:" + str(self.cli.returncode))
        logging.debug("error:")
        logging.debug(err)
        if self.crash_signal(err):
            logging.warning(f"return code: {self.cli.returncode}")
            logging.warning(f"err: {err}")
            logging.warning(self.input)
            status = False
        self.cli.terminate()
        return status

    def fuzz(self, path: str) -> None:
        random.seed(self.seed)
        if os.path.isdir(path):
            self.load_suite(path)
        else:
            self.load(path)
        self.setup_summary()
        # print time
        # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        
        test_cases = list(self.test_cases['TESTFILE_PATH'].unique())
        if logging.getLogger().level == logging.DEBUG:
            random.shuffle(test_cases)
        for test_case in test_cases:
            # logging.info(f"Running iteration ")
            self.extract(test_case)

            for i in self.iter_times:
                self.init_db('fuzzing')
                self.mutate(non = (self.mode == "simple"))
                signal.signal(signal.SIGALRM, self.timeout_handler)
                signal.alarm(self.MAX_TIME_PER_RUN)
                try:
                    status = self.run()
                except TimeoutError:
                    logging.warning(f"Timeout for test case {test_case}")
                    logging.info(self.input)
                    self.cli.terminate()
                    break
                else:
                    signal.alarm(0)
                if status == False:
                    break
                if self.mode == "simple":
                    break
            if logging.getLogger().level == logging.DEBUG:
                exit(0)


class SQLiteSimpleFuzzer(SimpleFuzzer):
    INT_POOL = [-1, 0, 1, 9223372036854775807, -
                9223372036854775808, 666, 112233, 14]
    SEG_POOL = [(-1, 1), (0, 1), (1, 14), (-9223372036854775807, -
                                           9223372036854775806), (9223372036854775806, 9223372036854775807), (-0.5, 0.5)]

    def __init__(self, seed=233, mode='mutation') -> None:
        super().__init__(seed, mode)
        self.cmd = [CONFIG['sqlite_path']]

    def filter(self, df) -> None: 
        # df = df[(df['SQL']).str.contains("generate_series") == False]
        df = df[df['STATUS'] == True]
        return df

class DuckDBSimpleFuzzer(SimpleFuzzer):
    INT_POOL = [-1, 0, 1, 9223372036854775807, -
                9223372036854775808, 666, 112233, 14]
    SEG_POOL = [(-1, 1), (0, 1), (1, 14), (-9223372036854775807, -
                                           9223372036854775806), (9223372036854775806, 9223372036854775807), (-0.5, 0.5)]

    def __init__(self, seed=233, mode='mutation') -> None:
        super().__init__(seed, mode)
        self.cmd = [CONFIG['duckdb_path']]


    def filter(self, df) -> None:
        df = df[(df['SQL']).str.contains("COPY") == False]
        # df = df[df['STATUS'] == True]
        return df

class PostgreSQLSimpleFuzzer(SimpleFuzzer):
    def __init__(self, seed, mode) -> None:
        super().__init__(seed, mode)
        self.cmd =  [
            'psql', f"postgresql://{CONFIG['postgres_user']}:{CONFIG['postgres_password']}@localhost:{CONFIG['postgres_port']}/fuzzing?sslmode=disable", '-X', '-q']


class MySQLSimpleFuzzer(SimpleFuzzer):
    def __init__(self, seed, mode) -> None:
        super().__init__(seed, mode)
        self.cmd =  [
            'mysql', '--force' ,'-u', CONFIG['mysql_user'], '-p' + CONFIG['mysql_password'], '-P', str(CONFIG['mysql_port']), '-h', '127.0.0.1', 'fuzzing']
        
    def crash_signal(self, err:str):
        return err.find("ERROR 200") != -1
    
    def init_db(self, db_name):
        init_mysql_script = f"""DROP DATABASE IF EXISTS {db_name}; CREATE DATABASE {db_name}; USE {db_name};"""
        # self.cmd += [db_name]
        cmd =  self.cmd[:-1] + ["-e", f"{init_mysql_script}"]
        logging.debug(f"init db {db_name}")
        logging.debug(cmd)
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.stderr.decode().find("ERROR") != -1:
            logging.error(f"err: {result.stderr.decode()}")
            # logging.error(self.input)
            exit(0)