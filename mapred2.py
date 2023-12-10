#!/bin/python3
"""mapper.py
https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/
"""

import sys
import re
import asyncio
import logging


async def connect_stdin_stdout():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    w_transport, w_protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, sys.stdout)
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    return reader, writer


# input comes from STDIN (standard input)
async def mapper(idx=None, sep=None, neval=None, eqval=None):
    logging.debug('enter mapper()')
    reader, writer = await connect_stdin_stdout()
    while True: #for line in sys.stdin:
        line = await reader.readline()
        if not line:
            break
        line = line.decode('utf-8')

        # remove leading and trailing whitespace
        line = line.strip("-_—.,?!\"'`’’“” \t")
        logging.debug("mapper.line=%s" % line)
        if sep is None:
            continue

        # split the line into words
        words = line.split(",")
        logging.debug("words=%s" % words)

        #filtration
        if (not (neval is None)) & (words[idx] == neval):
            continue
        if (not (eqval is None))  & (words[idx] != eqval):
            continue
        # produce the mapper result
        writer.write(bytes('%s\t%s\n' % (words[idx], 1), 'utf-8'))
        await writer.drain()


async def mapper_asis():
    logging.debug('enter mapper_asis()')
    reader, writer = await connect_stdin_stdout()
    while True: #for line in sys.stdin:
        line = await reader.readline()
        if not line:
            break

        line = line.decode('utf-8')
        logging.debug('%s\t1' % line)

        # produce mapper result as as-is copy of input with weight=1
        writer.write(bytes('%s\t1\n' % line, 'utf-8'))
        await writer.drain()


async def reducer():
    logging.debug('enter reducer()')
    reader, writer = await connect_stdin_stdout()
    current_word =  None
    current_count = 0
    word = None
    count = 0
    # input comes from STDIN
    while True: #for line in sys.stdin:
        line = await reader.readline()
        if not line:
            break

        line = line.decode('utf-8')
        # remove leading and trailing whitespace
        line = line.strip()

        try:
            # parse the input we got from mapper.py
            word, count = line.split('\t', 1)

            # convert count (currently a string) to int
            count = int(count)
        except ValueError:
            # count was not a number, so silently ignore/discard this line
            continue

        # this IF-switch only works because Hadoop sorts map output
        # by key (here: word) before it is passed to the reducer
        if current_word == word:
            current_count += count
        else:
            if current_word:
                # write result to STDOUT
                #print( '%s\t%s' % (current_word, current_count))
                writer.write(bytes('%s\t%s\n' % (current_word, current_count), 'utf-8'))
                await writer.drain()
            current_count = count
            current_word = word

    # do not forget to output the last word if needed!
    if current_word == word:
        #print( '%s\t%s' % (current_word, current_count))
        writer.write(bytes('%s\t%s\n' % (current_word, current_count), 'utf-8'))
        await writer.drain()

async def reducer_max(getmax):
    logging.debug('enter reducer_max()')
    reader, writer = await connect_stdin_stdout()
    current_word =  None
    current_count = 0
    word = None
    count = 0
    res = 0
    if not getmax:
        res = 999999 #todo: max(int)
    # input comes from STDIN
    while True: #for line in sys.stdin:
        line = await reader.readline()
        if not line:
            break

        line = line.decode('utf-8')
        # remove leading and trailing whitespace
        line = line.strip()

        try:
            # parse the input we got from mapper.py
            word, count = line.split('\t', 1)

            # convert count (currently a string) to int
            count = int(count)
        except ValueError:
            # count was not a number, so silently ignore/discard this line
            continue

        # this IF-switch only works because Hadoop sorts map output
        # by key (here: word) before it is passed to the reducer
        if current_word == word:
            current_count += count
        else:
            if current_word:
                # execute MAX or MIN logic
                if (getmax) & (res < current_count):
                    res = current_count
                elif (not getmax) & (res > current_count):
                    res = current_count 
            current_count = count
            current_word = word

    # do not forget to output the last word if needed!
    if current_word == word:
        if (getmax) & (res < current_count):
            res = current_count
        elif (not getmax) & (res > current_count):
            res = current_count

    # produce result
    #print('%s result\t%s' % ("Max" if getmax else "Min", res))
    writer.write(bytes('%s result\t%s\n' % ("Max" if getmax else "Min", res), 'utf-8'))
    await writer.drain()

params = {}
n = None
for v in sys.argv:
  if v in ("-idx","-sep","-ne","-eq"):
    n = v
  elif n != None:
    params[n] = v
    n = None
  else:
    n = None

def getint(name, default):
  v = params.get(name,None)
  if v is None:
    return default
  else:
    return int(v)

if __name__ == "__main__":
    logging.basicConfig(filename='/tmp/hadoop/az.log', encoding='utf-8', level=logging.DEBUG)
    try:
        if "-map" in sys.argv:
            asyncio.run(mapper(getint('-idx', None), params.get('-sep', None), params.get('-ne',None), params.get('-eq',None)))
        elif "-asis" in sys.argv:
            asyncio.run(mapper_asis())
        elif "-reduce" in sys.argv:
            asyncio.run(reducer())
#        elif "-reduce_max"in sys.argv:
#            asyncio.run(reducer_max(params.get("-max",True)))
        elif "-max" in sys.argv:
            asyncio.run(reducer_max(True))
        elif "-min" in sys.argv:
            asyncio.run(reducer_max(False))
    except KeyboardInterrupt:
        pass

