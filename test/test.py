import os
import queue
import subprocess
import threading
import time
from enum import Enum
from typing import Any, Dict

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
]


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"


class TestResult(Enum):
    PASS = "PASS"
    FAIL = "FAIL"


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
    color = Colors.GREEN if result == TestResult.PASS else Colors.RED
    status = f"{color}{result.value}{Colors.END}"
    print(f"{Colors.BOLD}{name}{Colors.END}: {status}")

    if error_message and result == TestResult.FAIL:
        print(error_message)
        print("-" * 80)


def run_server_test(test_case: Dict[str, Any]) -> bool:
    server = ServerProcess()
    success = True
    error_messages = []

    try:
        server.start(test_case["cmd"])
        time.sleep(0.5)

        command_outputs = []
        for cmd in test_case["redis_commands"]:
            output = run_redis_command(cmd)
            command_outputs.append(output)
            time.sleep(0.1)

        server_output = server.stop()

        if test_case["output"] != "":
            server_success, error_msg = compare_outputs(
                server_output, test_case["output"], "Server output"
            )
            if not server_success:
                success = False
                error_messages.append(error_msg)
                server.print_captured_output()

        for i, (output, expected) in enumerate(
            zip(command_outputs, test_case["command_outputs"])
        ):
            cmd_success, error_msg = compare_outputs(
                output, expected, f"Command '{test_case['redis_commands'][i]}' output"
            )
            if not cmd_success:
                success = False
                error_messages.append(error_msg)

        return success, "\n".join(error_messages) if error_messages else None

    except Exception as e:
        error_message = f"Test failed with error: {e}"
        server.print_captured_output()
        server.stop()
        return False, error_message


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
    for case in cases:
        success, error_message = run_server_test(case)
        all_tests_passed &= success
        print_test_result(
            case["name"], TestResult.PASS if success else TestResult.FAIL, error_message
        )

    print("\n" + "=" * 80)
    summary_color = Colors.GREEN if all_tests_passed else Colors.RED
    summary_text = "All tests passed!" if all_tests_passed else "Some tests failed!"
    print(f"\n{Colors.BOLD}Test Summary: {summary_color}{summary_text}{Colors.END}")
