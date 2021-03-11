import subprocess
import sys

MAX_READLINES = 1000


class Sinks:

    def __init__(self, sink_commands):
        self.sinks = []
        self.lines_sent = 0
        for idx, param in enumerate(sink_commands):
            print(f'Executing {idx + 1}: {param}')
            self.sinks.append(subprocess.Popen(param.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT))

    def write_lines(self, lines):
        self.lines_sent += len(lines)
        encoded_lines = [line.encode('utf-8') for line in lines]
        [sink.stdin.writelines(encoded_lines) for sink in self.sinks]

    def print_errors(self):
        for idx, sink in enumerate(self.sinks):
            (out, err) = sink.communicate()
            if out:
                print(f'Output in {idx + 1}: {out}')
            if err:
                print(f'Error in {idx + 1}: {err}')


sinks = Sinks(sys.argv[1:])

input_lines = sys.stdin.readlines(MAX_READLINES)
while input_lines:
    sinks.write_lines(input_lines)
    input_lines = sys.stdin.readlines(MAX_READLINES)

sinks.print_errors()

print(f"{sinks.lines_sent} lines sent")
