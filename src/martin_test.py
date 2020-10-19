import os
import glob
from pathlib import Path

def is_file(path):
    splitted_path = file.name.split('/')
    for i in range(2):
      splitted_path.pop()
    joined_path = '/'.join(splitted_path)
    return os.path.isfile('%s/labels.txt' % joined_path)
  

path = Path(__file__).parent.absolute() # path = Path(__file__).parent.parent.absolute()
for filepath in glob.iglob(r'%s/**/*.plt' % path, recursive=True):
    with open(filepath, 'r') as file:
        lines = file.readlines()

        if len(lines) > 3: # 2500
          continue

        #Activity = {}
        folder_contains_label_file = is_file(path.name)
        if folder_contains_label_file:
          startTime = lines[0]
          endTime = lines[-1]

        c = 1
        for line in lines:
          if c <= 0: # 6
              c += 1
              continue
          data = line.strip()
          #print(data)
