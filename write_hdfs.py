import pydoop.hdfs as hdfs
with hdfs.open('~test', 'w') as f:
    for line in ("123","abc"):
        f.write(line)


