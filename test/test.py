import os
import subprocess

current_file_path = os.path.dirname(os.path.abspath(__file__))


def run_command_and_check_output(command, expected_output):
    try:
        # Run the command and capture the output
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            text=True,
            capture_output=True,
            cwd=current_file_path,
        )
        output = result.stdout.strip()

        # Compare the output with the expected output
        if output == expected_output.strip():
            print("Output matches expected output.")
        else:
            print("output: ", output)
            print("Output does not match expected output.")
            print("Differences found:")
            expected_lines = expected_output.splitlines()
            output_lines = output.splitlines()
            for i, (expected_line, output_line) in enumerate(
                zip(expected_lines, output_lines), start=1
            ):
                if expected_line != output_line:
                    print(f"Line {i}:")
                    print(f"Expected: `{expected_line}`")
                    print(f"Got:      `{output_line}`")
                    print()
            if len(expected_lines) != len(output_lines):
                print("Output has different number of lines than expected.")

    except subprocess.CalledProcessError as e:
        print(f"Command failed with error: {e}")


cases = [
    {
        "cmd": "./bin --dir . --dbfilename dump.rdb --test-rdb",
        "output": """dir: .
dbfilename: dump.rdb
loaded rdb content:
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
    }
]

if __name__ == "__main__":
    result = subprocess.run(
        "OUTPUT=test/bin make build",
        shell=True,
        check=True,
        text=True,
        capture_output=True,
    )
    print("build finished")
    for case in cases:
        run_command_and_check_output(case["cmd"], case["output"])
