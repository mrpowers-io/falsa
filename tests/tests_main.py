import os
import sys
import hashlib
from subprocess import Popen, PIPE, STDOUT


SHA256SUM_CSV_SMALL = "bab41ac749ed0d884a89b9b87869c7ce1d41ce8b8eb48819be06da8c274b5a04"
CSV_FILE_NAME = "G1_1e7_1e7_100_0.csv"

if __name__ == "__main__":
    command = ["falsa", "groupby", "--path-prefix", "./"]
    proc = Popen(command, stdout=PIPE, stderr=STDOUT)
    res = proc.communicate()
    if not os.path.exists(CSV_FILE_NAME):
        sys.stdout.write("File was not generated! Aborting.")
        sys.stdout.flush()
        sys.exit(1)

    hash = ""
    with open(CSV_FILE_NAME, "rb", buffering=0) as file_:
        hash = hashlib.file_digest(file_, "sha256").hexdigest()

    if not SHA256SUM_CSV_SMALL == hash:
        sys.stdout.write(f"Wrong hash. Expected {SHA256SUM_CSV_SMALL} but got {hash}")
        sys.stdout.flush()
        sys.exit(1)

    sys.exit(0)
