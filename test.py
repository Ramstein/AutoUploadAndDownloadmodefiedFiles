
import os

work_directory = r"C:\Users\ZEESHAN ALAM\Documents\SC"

for dirname, subdirs, files in os.walk(work_directory):

    print(dirname, subdirs, files)
