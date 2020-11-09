import time, sys,re, traceback, os,string
from concurrent.futures import ThreadPoolExecutor

import logging, threading

#todo config file
#todo extra thread for progress
#todo delimiter for words that define begin and end



############################ CONFIG
CASE_INSENSITIVE = True
BUFSIZE = 1024*1024*4
NUM_THREADS = 1
START_FILENUM = 0
PATH = '/media/sf_VMshare/PoolTagFinder' # search in all files and in subfolders
NUM_PREVIEW_CHARS = 70

READBYTES_TO_PRINT_PROGRESS = 1024*1024*16


IMPORT_KEYWORDS = False
PATH_IMPORT_KEYWORDS = '/home/user/keywords.txt'

allowedExt = [".txt", ".csv", ".sql", ".db", ".html"]

searchPatterns = ["bla", "test"]
regexPatterns = ["test.?", "bla.?"]

############################ GLOBALS
global g_totalGigaReadBytes
g_logger = ""
g_resultLogger = ""
g_fileCounter = 0
g_longestPatternCount = 0
g_totalReadGigaBytes = 0

g_jobList = []

############################ CLASSES

class Job:
    def __init__(self,filepath,fileNo):
        self.readBytesProgressBuffer = READBYTES_TO_PRINT_PROGRESS
        self.progress = 0
        self.filepath = filepath
        self.fileNo = fileNo
        self.readTotalBytes = 0
        self.readBytes = 0
        self.filesize = 0
        self.threadName = ""


    def updateProgress(self):
        global g_totalReadGigaBytes
        progress = round(self.readTotalBytes / self.filesize * 100, 2)

        self.readBytesProgressBuffer -= self.readBytes
        if self.readBytesProgressBuffer < 0:
            self.readBytesProgressBuffer = READBYTES_TO_PRINT_PROGRESS
            print("Thread: " + self.threadName + " | Progress: " + str(progress) + "% | TotalReadGigaBytes: " + str(g_totalGigaReadBytes) + " | Path: " + safePrint(self.filepath))

############################ FUNCTIONS
def safePrint(text):
    valid_chars = "/-_.() %s%s" % (string.ascii_letters, string.digits)
    return ''.join(c for c in text if c in valid_chars)


def importKeywords(path):
    file = open(path, "r")
    lines = file.readlines()
    for line in lines:
        if line.strip().replace("\n","") != "":
            if (CASE_INSENSITIVE):
                searchPatterns.append(line.lower().replace("\n",""))
            else:
                searchPatterns.append(line.replace("\n",""))

def init():
    g_longestPatternCount = 0
    for pattern in searchPatterns:
        if g_longestPatternCount < len(pattern):
            g_longestPatternCount = len(pattern)

    if (CASE_INSENSITIVE):
        for i in range(0, len(searchPatterns)):
            searchPatterns[i] = searchPatterns[i].lower()

def hasThisExtension(filename, ext):
    return ext == filename[-len(ext):]

def readBufferedLines(file):
    if(g_longestPatternCount > BUFSIZE):
        global g_logger
        g_logger.critical("BUFSIZE to small")
        sys.exit()

    text = file.read(BUFSIZE)
    if((g_longestPatternCount < len(text)) and (file.tell() != 0)):
        file.seek(-g_longestPatternCount,os.SEEK_CUR) #set file position a bit back for the case that if the searched pattern was not read completely in the previous iteration

    return text

def worker(job):
    try:
        searchFile(job)
    except Exception as e:
        global g_logger
        errmsg = traceback.format_exc()
        g_logger.critical(errmsg)
        print(g_jobList)

def searchFile(job):
    global g_totalGigaReadBytes
    global g_logger
    global g_resultLogger


    job.filesize = os.path.getsize(job.filepath)
    job.threadName = threading.currentThread().getName()

    g_logger.warning("Begin search - " + str(job.fileNo) + ' Size: ' + str(round((job.filesize/1024)/1024,4)) + ' MiB | Path: ' + safePrint(job.filepath))


    file = open(job.filepath, "rb")
    g_logger.warning("Thread: " + job.threadName + " - " +str(job.fileNo) + " - Begin ... | Path: " + safePrint(job.filepath))

    reachedNotEOF = True
    while(reachedNotEOF):
        #fPosBeforeRead = file.tell()
        text = readBufferedLines(file)
        job.readTotalBytes += len(text)
        job.readBytes = len(text)

        if (len(text) < BUFSIZE):
            g_logger.warning("Thread: " + job.threadName + " | TotalReadGigaBytes: " + str(g_totalGigaReadBytes) +  " | Reached EOF" + " | Path: " + safePrint(job.filepath))
            reachedNotEOF = False

        text = text.decode(errors="ignore")

        if (CASE_INSENSITIVE):
            text = text.lower()
        #search

        logMsgThread = "Thread: " + job.threadName
        job.updateProgress()


        if len(searchPatterns) > 0:
            for pattern in searchPatterns:
                pos = text.find(pattern)

                while (pos != -1):
                    msg = logMsgThread +  " | FOUND String: " + pattern + " | Preview: ... " # " | Progress: " + str(progress)

                    PREVIEW_LIMIT = NUM_PREVIEW_CHARS
                    if (pos-PREVIEW_LIMIT) >= 0 and (pos + len(pattern)+PREVIEW_LIMIT) < len(text): # is in "middle" of buffer
                         msg += text[pos-PREVIEW_LIMIT:pos+len(pattern)+PREVIEW_LIMIT].replace("\n", "\\\\N").replace("\r", "\\\\R")

                    elif (pos-PREVIEW_LIMIT) < 0 and (pos + len(pattern)+PREVIEW_LIMIT) < len(text): # is very near to begin of buffer
                        msg += text[0:pos+len(pattern)+PREVIEW_LIMIT].replace("\n", "\\\\N").replace("\r", "\\\\R")

                    elif (pos - PREVIEW_LIMIT) >= 0 and (pos + len(pattern) + PREVIEW_LIMIT) >= len(text): # is near to end of buffer
                        msg += text[pos-PREVIEW_LIMIT:].replace("\n", "\\\\N").replace("\r", "\\\\R")

                    elif (pos - PREVIEW_LIMIT) < 0 and (pos + len(pattern) + PREVIEW_LIMIT) >= len(text): # is near begin and end of buffer (because small text)
                        msg += text.replace("\n", "\\\\N").replace("\r", "\\\\R")

                    msg += " ... | Path: " + job.filepath

                    g_logger.warning(msg)
                    g_resultLogger.warning(msg)

                    pos = text.find(pattern, pos + len(pattern))  # continue search in same buffer


        if len(regexPatterns) > 0:
            flags = 0
            if (CASE_INSENSITIVE):
                flags = re.IGNORECASE

            for pattern in regexPatterns:
                matches = re.findall(pattern, text, flags)
                if len(matches) > 0:
                    msg = logMsgThread  + " | FOUND Pattern: " + pattern + " | Regex Matches: " + str(matches) + " | Path: " + safePrint(job.filepath)
                    g_logger.warning(msg)
                    g_resultLogger.warning(msg)

    file.close()


    g_totalGigaReadBytes += ((job.readTotalBytes/1024)/1024)/1024

    g_jobList.remove(job)

def createLogger():
    logger = logging.getLogger(__name__)
    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler("log_" + time.strftime("%Y-%m-%d_%H-%M-%S") + '.txt')
    c_handler.setLevel(logging.DEBUG)
    f_handler.setLevel(logging.DEBUG)
    # Create formatters and add it to handlers
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    c_handler.setFormatter(format)
    f_handler.setFormatter(format)
    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    return logger

def createFileLogger():
    logger = logging.getLogger("resultlogger")
    f_handler = logging.FileHandler("result_" + time.strftime("%Y-%m-%d_%H-%M-%S") + '.txt')
    f_handler.setLevel(logging.DEBUG)
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    f_handler.setFormatter(format)
    logger.addHandler(f_handler)
    return logger



def main():
    global g_logger
    g_logger = createLogger()
    g_logger.warning('Start ...')

    global g_resultLogger
    g_resultLogger = createFileLogger()

    global g_totalGigaReadBytes
    g_totalGigaReadBytes = 0
    fileCounter = 0

    executor = ThreadPoolExecutor(max_workers=NUM_THREADS)

    init()

    if(IMPORT_KEYWORDS):
        if not os.path.isfile(PATH_IMPORT_KEYWORDS):
            g_logger.error("Keywords file '%s' not found! Exit ...", PATH_IMPORT_KEYWORDS)
            sys.exit()

        importKeywords(PATH_IMPORT_KEYWORDS)

    path = PATH
    g_logger.warning("searchPatterns: " + str(searchPatterns))
    g_logger.warning("regexPatterns: " + str(regexPatterns))

    if not os.path.isdir(PATH):
        g_logger.error("Directory '%s' not found! Exit ...",PATH)
        sys.exit()

    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        print(r)
        print(d)
        print(f)
        for filename in f: #iterate over all txt's

            #global g_fileCounter


            if (START_FILENUM > fileCounter):
                continue

            if filename.find(".") == -1: # check if filename has extension
                filepath = os.path.join(r, filename)
                g_logger.error("File has no extension: " + safePrint(filepath))
                continue

            hasAllowedExt = False
            for ext in allowedExt:
                if hasThisExtension(filename.lower(), ext): #convert for check to lower case (filename extension)
                    filepath = os.path.join(r, filename)

                    slotIndex = len(g_jobList)
                    job = Job(filepath,fileCounter)
                    g_jobList.append(job)

                    executor.submit(worker, job)
                    hasAllowedExt = True
                    fileCounter += 1
                    break #break inner loop

            if not hasAllowedExt:
                path = os.path.join(r, filename)
                g_logger.warning('Skip (Not Allowed Extension)...: ' + safePrint(path))
                continue  # skip

    executor.shutdown()
    g_logger.warning('Done!')



########################### MAIN
print("START")
main()

print("DONE")