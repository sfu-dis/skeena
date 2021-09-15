import re
import os
from operator import itemgetter
import argparse

result_tbl = {}
percentages = []

parser = argparse.ArgumentParser(description="Kaisong's log parser")
parser.add_argument("--timestamp", dest="begin_ts", required=True)
parser.add_argument("--startswith", dest="startswith", required=True)
args = parser.parse_args()
begin_ts = int(args.begin_ts)
startswith = args.startswith

directory = './'
for root, dirs, files in os.walk(directory):
  files.sort()
  for filename in files:
    if filename.endswith(".log"):
      pattern = "(.*)_(\d+)-inno_(\d+)@(\d+)\.\d+.log"      
      result = re.match(pattern, filename)
      bench, conns, percentage, ts = result.groups()

      if percentages.count(int(percentage)) == 0:
          percentages.append(int(percentage))

      # Skip old logs
      if int(ts) < begin_ts:
          continue

      if bench not in result_tbl:
          result_tbl[bench] = []

      filepath = os.path.join(directory, filename)
      f = open(filepath, "r")

      extracted = []
      for line in f:
        
        if line.startswith(startswith):
          if startswith == '         min:' or \
             startswith == '         avg:' or \
             startswith == '         max:' or \
             startswith == '         95th' :
            try:
              res = re.findall("\d+\.\d+", line)
              found = float(res[0])
            except AttributeError:
              found = 0
          if startswith == '    queries:' or \
             startswith == '    ignored'  or \
             startswith == '    transactions' :
            try:
              found = float(re.search('\((.+?) per sec', line).group(1))
            except AttributeError:
              found = 0
          extracted.append(found)
       
      formula = '=('
      for i, item in enumerate(extracted):
        if i:
          formula += '+'
        formula += str(item)
      formula += ')/' + str(len(extracted))

      result_tbl[bench].append((int(conns), int(percentage), formula))
    else:
      continue
  break

# Construct the CSV result by self
for bench, items in result_tbl.items():
    print(bench)
    header = "# of connections"
    percentages.sort()
    for percentage in percentages:
        header = header + ",{}% InnoDB".format(percentage)
    print(header)
    max_percentage = percentages[-1]
    items.sort(key=itemgetter(1))
    items.sort(key=itemgetter(0))
    line, pre_thd = "", 0
    for item in items:
        thd, percentage, formula = item
        if pre_thd != thd:
            if line != "":
                print(line)
            line = ""
            if line == "":
                line = "{}".format(thd)
            pre_thd = thd
        line = line + ",{}".format(formula)
    print(line)
