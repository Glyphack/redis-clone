import subprocess


def run_command_and_check_output(command, expected_output):
    try:
        # Run the command and capture the output
        result = subprocess.run(
            command, shell=True, check=True, text=True, capture_output=True
        )
        output = result.stdout.strip()

        # Compare the output with the expected output
        if output == expected_output.strip():
            print("Output matches expected output.")
        else:
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


if __name__ == "__main__":
    command = "make build && ./a.out --dir . --dbfilename dump.rdb"
    expected_output = """dir: .
dbfilename: dump.rdb
config:
  dir `.`
  dbfilename `dump.rdb`
reading header
reading metadata
reading database section
reading key val section
key type string
parsing RDB finished
header is
REDIS0011
auxiliary fields
redis-ver
7.2.6
redis-bits
64
ctime
1734275836
used-mem
1148496
aof-base
0
databases
database number: 0
hash size: 1
expiry size: 0
key : mykey
value : myval""".strip()
    run_command_and_check_output(command, expected_output)
