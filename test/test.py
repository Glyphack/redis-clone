import os
import queue
import signal
import subprocess
import threading
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict


@contextmanager
def timeout(seconds):
    def signal_handler(signum, frame):
        raise TimeoutError("Timed out")

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


cases = [
    {
        "name": "check replication output",
        "cmd": "./bin --port 9999",
        "output": "",
        "redis_commands": ["echo 'INFO replication' | redis-cli -p 9999"],
        "command_outputs": [
            """role:master
master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
master_repl_offset:0""".strip()
        ],
    },
    {
        "name": "check load RDB",
        "cmd": "./bin --port 9999 --dir . --dbfilename dump.rdb --test-rdb",
        "output": """loaded rdb content:
header is
REDIS0011
auxiliary fields
redis-ver
7.2.6
redis-bits
64
ctime
-11
used-mem
-6336
aof-base
0
databases
database number: 0
hash size: 3
expiry size: 1
key : mykey2
value : val2
expiration: -1
key : mykey
value : val
expiration: -1
key : key
value : value
expiration: 1735925226569""".strip(),
        "redis_commands": [],
        "command_outputs": [],
    },
    {
        "name": "handle large request",
        "cmd": "./bin --port 9999",
        "output": "",
        "redis_commands": [f"redis-cli -p 9999 SET {'x'*5000} {'y'*5000}"],
        "command_outputs": ["OK"],
    },
    {
        "name": "test ECHO command",
        "cmd": "./bin --port 9999",
        "output": "",
        "redis_commands": ["redis-cli -p 9999 ECHO 'Hello World'"],
        "command_outputs": ["Hello World"],
    },
    {
        "name": "test SET and GET commands",
        "cmd": "./bin --port 9999",
        "output": "",
        "redis_commands": [
            "redis-cli -p 9999 SET mykey myvalue",
            "redis-cli -p 9999 GET mykey",
            "redis-cli -p 9999 GET nonexistent",
        ],
        "command_outputs": ["OK", "myvalue", ""],
    },
    {
        "name": "test SET with expiry",
        "cmd": "./bin --port 9999",
        "output": "",
        "redis_commands": [
            "redis-cli -p 9999 SET key1 value1 px 100",
            "redis-cli -p 9999 GET key1",
            "sleep 0.2",  # Wait for key to expire
            "redis-cli -p 9999 GET key1",
        ],
        "command_outputs": ["OK", "", ""],
    },
    {
        "name": "test KEYS command",
        "cmd": "./bin --port 9999",
        "output": "",
        "redis_commands": [
            "redis-cli -p 9999 SET key1 val1",
            "redis-cli -p 9999 SET key2 val2",
            "redis-cli -p 9999 KEYS '*'",
        ],
        "command_outputs": ["OK", "OK", "key1\nkey2"],
    },
    {
        "name": "test PING command",
        "cmd": "./bin --port 9999",
        "output": "",
        "redis_commands": ["redis-cli -p 9999 PING"],
        "command_outputs": ["PONG"],
    },
    {
        "name": "test CONFIG GET command",
        "cmd": "./bin --port 9999 --dir /tmp --dbfilename test.rdb",
        "output": "",
        "redis_commands": [
            "redis-cli -p 9999 CONFIG GET dir",
            "redis-cli -p 9999 CONFIG GET dbfilename",
        ],
        "command_outputs": [
            """dir
/tmp""",
            """dbfilename
test.rdb""",
        ],
    },
    # fails because replication mode requires ctrl-c to quit and this test hangs
    # {
    #     "name": "test handshake commands",
    #     "cmd": "./bin --port 9999",
    #     "output": "",
    #     "redis_commands": [
    #         "redis-cli -p 9999 REPLCONF listening-port 6380",
    #         "redis-cli -p 9999 REPLCONF capa psync2",
    #         "redis-cli -p 9999 PSYNC '?' '-1'",
    #     ],
    #     "command_outputs": [
    #         "OK",
    #         "OK",
    #         "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0",
    #     ],
    # },
]


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"


class TestResult(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    TIMEOUT = "TIMEOUT"


current_file_path = os.path.dirname(os.path.abspath(__file__))


class ServerProcess:
    def __init__(self):
        self.process = None
        self.output_queue = queue.Queue()
        self.should_stop = False
        self.captured_output = []

    def _output_reader(self, pipe):
        while not self.should_stop:
            line = pipe.readline()
            if not line:
                break
            self.captured_output.append(f"[server] {line}")
            self.output_queue.put(line)

    def start(self, command):
        self.process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            cwd=current_file_path,
        )
        self.output_thread = threading.Thread(
            target=self._output_reader, args=(self.process.stdout,)
        )
        self.output_thread.daemon = True
        self.output_thread.start()

    def stop(self):
        if self.process:
            self.should_stop = True
            self.process.terminate()
            self.process.wait()
            self.output_thread.join(timeout=1)
            return self.get_output()

    def get_output(self):
        output = []
        while True:
            try:
                line = self.output_queue.get_nowait()
                output.append(line)
            except queue.Empty:
                break
        return "".join(output)

    def print_captured_output(self):
        print("\nServer Output:")
        print("-" * 50)
        for line in self.captured_output:
            print(line, end="")
        print("-" * 50)


def run_redis_command(command):
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            text=True,
            capture_output=True,
            cwd=current_file_path,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(
            f"{Colors.RED}Redis command '{command}' failed with error: {e}{Colors.END}"
        )
        return None


def compare_outputs(actual, expected, description=""):
    if actual.strip() == expected.strip():
        return True, None

    error_message = []
    error_message.append(f"\n{description} - Output does not match expected output.")
    error_message.append("Differences found:")

    expected_lines = expected.strip().splitlines()
    actual_lines = actual.strip().splitlines()

    error_message.append(f"\nExpected:\n{expected}")
    error_message.append(f"\nActual:\n{actual}")

    for i, (expected_line, actual_line) in enumerate(
        zip(expected_lines, actual_lines), start=1
    ):
        if expected_line != actual_line:
            error_message.append(f"\nLine {i}:")
            error_message.append(f"Expected: `{expected_line}`")
            error_message.append(f"Got:      `{actual_line}`")

    if len(expected_lines) != len(actual_lines):
        error_message.append("\nOutput has different number of lines than expected.")

    return False, "\n".join(error_message)


def print_test_result(name: str, result: TestResult, error_message: str = None):
    if result == TestResult.PASS:
        status = f"{Colors.GREEN}{result.value}{Colors.END}"
    elif result == TestResult.TIMEOUT:
        status = f"{Colors.YELLOW}{result.value}{Colors.END}"
    else:
        status = f"{Colors.RED}{result.value}{Colors.END}"

    print(f"{Colors.BOLD}{name:<50}{Colors.END} {status}")

    if error_message and result != TestResult.PASS:
        print(error_message)
        print("-" * 80)


def run_server_test(test_case: Dict[str, Any]) -> tuple[TestResult, str]:
    server = ServerProcess()
    print(f"{Colors.BOLD}Running: {test_case['name']}{Colors.END}")

    try:
        server.start(test_case["cmd"])
        time.sleep(0.5)

        command_outputs = []

        try:
            with timeout(3):
                for cmd in test_case["redis_commands"]:
                    output = run_redis_command(cmd)
                    command_outputs.append(output)
                    time.sleep(0.1)
        except TimeoutError:
            server.stop()
            return TestResult.TIMEOUT, f"Command '{cmd}' timed out after 1 second"

        server_output = server.stop()

        if test_case["output"] != "":
            server_success, error_msg = compare_outputs(
                server_output, test_case["output"], "Server output"
            )
            if not server_success:
                return TestResult.FAIL, error_msg

        for i, (output, expected) in enumerate(
            zip(command_outputs, test_case["command_outputs"])
        ):
            cmd_success, error_msg = compare_outputs(
                output, expected, f"Command '{test_case['redis_commands'][i]}' output"
            )
            if not cmd_success:
                return TestResult.FAIL, error_msg

        return TestResult.PASS, None

    except Exception as e:
        error_message = f"Test failed with error: {e}"
        server.print_captured_output()
        server.stop()
        return TestResult.FAIL, error_message


if __name__ == "__main__":
    print(f"\n{Colors.BOLD}Running Redis Implementation Tests{Colors.END}")
    print("=" * 80)

    print(f"\n{Colors.BOLD}Building program...{Colors.END}")
    try:
        result = subprocess.run(
            "OUTPUT=test/bin make build -B",
            shell=True,
            check=True,
            text=True,
            capture_output=True,
        )
        print(f"{Colors.GREEN}Build successful{Colors.END}")
    except subprocess.CalledProcessError as e:
        print(f"{Colors.RED}Build failed: {e}{Colors.END}")
        exit(1)

    print("\n" + "=" * 80)

    all_tests_passed = True
    total_tests = len(cases)
    passed_tests = 0
    failed_tests = 0
    timeout_tests = 0

    for case in cases:
        result, error_message = run_server_test(case)
        if result == TestResult.PASS:
            passed_tests += 1
        elif result == TestResult.TIMEOUT:
            timeout_tests += 1
            all_tests_passed = False
        else:
            failed_tests += 1
            all_tests_passed = False

        print_test_result(case["name"], result, error_message)

    print("\n" + "=" * 80)
    print(f"\n{Colors.BOLD}Test Summary:{Colors.END}")
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {Colors.GREEN}{passed_tests}{Colors.END}")
    if failed_tests > 0:
        print(f"Failed: {Colors.RED}{failed_tests}{Colors.END}")
    if timeout_tests > 0:
        print(f"Timeouts: {Colors.YELLOW}{timeout_tests}{Colors.END}")

    summary_color = Colors.GREEN if all_tests_passed else Colors.RED
    summary_text = "All tests passed!" if all_tests_passed else "Some tests failed!"
    print(f"\n{Colors.BOLD}Final Result: {summary_color}{summary_text}{Colors.END}")
