import pandas as pd
import logging
import subprocess
import re
import random
import signal
import time
from tqdm import tqdm
import string

# MAXINT in SQLite is 9223372036854775807
# from data import filters

class Fuzzer():
    MAX_TIME_PER_RUN = 5
    def __init__(self, seed:int) -> None:
        self.test_cases:pd.DataFrame = None
        self.sql_list = []
        self.input = ""
        self.seed = seed
    
    def timeout_handler(self, signum, frame):
        raise TimeoutError
    
    def run(self, command: str) -> None:
        subprocess.run(command, shell=True)
        
    def load(self, path: str) -> pd.DataFrame:
        self.test_cases = pd.read_csv(path)
    
    def extract(self) -> None:
        self.sql_list = self.test_cases['SQL'].tolist()
    
    def mutate(self) -> None:
        pass
    
    
class SimpleFuzzer(Fuzzer):
    # MAX_INT_POOL = [-9223372036854775808, -1, 0, 1, 9223372036854775807]
    INT_POOL = [-1, 0, 1, 9223372036854775807, -9223372036854775808]
    SEG_POOL = [(-1, 1), (0, 1), (1, 14),  (-0.5, 0.5)]
    
    FUZZING_TAG = ["INT", "NUMERIC", "STR"]
    
    def __init__(self, seed) -> None:
        super().__init__(seed)
        self.cmd = []
        self.cli = None
        self.iter_times = tqdm(range(100)) if logging.getLogger().getEffectiveLevel() == logging.DEBUG else range(3000)
    
    def setup_summary(self):
        logging.info("Setup Summary")
        logging.info(f"Fuzzer: {self.__class__.__name__}")
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
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(random.randint(1, 20)))
    
    def constant_mutator(self, sql: str, tag) -> str:
        return re.sub(r"(\b\d+(\.\d+)?\b)|('(?:[^']|'')*')", self._random_generator(tag), sql)
    
    
    def mutate(self, iter = False) -> None:
        # replace every number in the sql with a random new number
        
        if iter:
            self.sql_list = [self.constant_mutator(str(sql), random.choice(self.FUZZING_TAG)) for sql in self.sql_list]
            self.input = "\n".join(self.sql_list)
        else:
            self.input = "\n".join([self.constant_mutator(str(sql), random.choice(self.FUZZING_TAG)) for sql in self.sql_list])
    
    def run(self) -> None:
        self.cli = subprocess.Popen(self.cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, encoding='utf-8', universal_newlines=True, bufsize=2 << 15)
        out, err = self.cli.communicate(input=self.input)
        logging.debug("input: ")
        logging.debug(self.input)
        logging.debug("output: ")
        logging.debug(out)
        if err:
            logging.warning(self.input)
            logging.warning(err)
        self.cli.terminate()
        
    def fuzz(self, path: str) -> None:
        random.seed(self.seed)
        self.load(path)
        self.setup_summary()
        # print time
        # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        
        if logging.getLogger().level == logging.DEBUG:
            self.test_cases = self.test_cases.sample(1)
        
        for test_case in self.test_cases['TESTFILE_PATH'].unique():
            # logging.info(f"Running iteration ")
            self.extract(test_case)
            
            for i in self.iter_times:
                self.mutate()
                signal.signal(signal.SIGALRM, self.timeout_handler)
                signal.alarm(self.MAX_TIME_PER_RUN)
                try:
                    self.run()
                except TimeoutError:
                    logging.warning(f"Timeout for test case {test_case}")
                    logging.info(self.input)
                    self.cli.terminate()
                    break
                else:
                    signal.alarm(0)
            if logging.getLogger().level == logging.DEBUG:
                exit(0)
        
class SQLiteSimpleFuzzer(SimpleFuzzer):
    INT_POOL = [-1, 0, 1, 9223372036854775807, -9223372036854775808, 666, 112233, 14]
    SEG_POOL = [(-1, 1), (0, 1), (1, 14), (-9223372036854775807, -
                                           9223372036854775806), (9223372036854775806, 9223372036854775807), (-0.5, 0.5)]
    
    def __init__(self, seed = 233) -> None:
        super().__init__(seed)
        self.cmd = ["sqlite3"]
    
    def extract(self, test_case) -> None:
        logging.info(f"Extracting test case {test_case}")
        df:pd.DataFrame = self.test_cases
        df = df[df['TESTFILE_PATH'] == test_case]
        self.sql_list = [str(sql).removesuffix(';') + ';' for sql in df['SQL'].tolist()]
        
        # filter generate_series
        self.sql_list = [sql for sql in self.sql_list if sql.find("generate_series") == -1]

class DuckDBSimpleFuzzer(SimpleFuzzer):
    INT_POOL = [-1, 0, 1, 9223372036854775807, -9223372036854775808, 666, 112233, 14]
    SEG_POOL = [(-1, 1), (0, 1), (1, 14), (-9223372036854775807, -
                                           9223372036854775806), (9223372036854775806, 9223372036854775807), (-0.5, 0.5)]
    
    def __init__(self, seed = 233) -> None:
        super().__init__(seed)
        self.cmd = ["scripts/duckdb"]
    
    def extract(self, test_case) -> None:
        logging.info(f"Extracting test case {test_case}")
        df:pd.DataFrame = self.test_cases
        df = df[df['TESTFILE_PATH'] == test_case]
        self.sql_list = [str(sql).removesuffix(';') + ';' for sql in df['SQL'].tolist()]