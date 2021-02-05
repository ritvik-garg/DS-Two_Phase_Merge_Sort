import sys
import math
import heapq
import time 
import os

col_isto_size = {}
col_index = {}
heap = []

# order_flag=True
# sort_acc_to_cols = []

class HeapNode:
    sort_col = []
    sort_order = None
    def __init__(self):
        self.data = []
        self.file_ptr = 0


    def __lt__(self, other):
        return compare(self.data, other.data, HeapNode.sort_col, HeapNode.sort_order)

def delete_temp_files(temp_filenames):
    for filename in temp_filenames:
        os.remove(filename)


def checkRec(temp_filenames):
    n = len(temp_filenames)
    res_len = []
    for i in range(n):
        f = open(temp_filenames[i])
        lines = f.readlines()
        res_len.append(len(lines))
    return res_len

def compare(l1,l2,columns,flag):
    if(flag):
        for j in columns:
            i=col_index[j]
            if(l1[i]>l2[i]):
                return True
            elif(l1[i]==l2[i]):
                continue
            else:
                return False
        return False
    else:
        for j in columns:
            i=col_index[j]
            if(l1[i]<l2[i]):
                return True
            elif(l1[i]==l2[i]):
                continue
            else:
                return False
        return False


def mergeFiles(temp_filenames, output_file, sort_acc_to_cols, order_flag):
    filepointer=[None]*len(temp_filenames)
    HeapNode.sort_col = sort_acc_to_cols
    HeapNode.sort_order = order_flag
    for i in range(len(temp_filenames)):
        filepointer[i]=open(temp_filenames[i])
        line = filepointer[i].readline()
        if line :
            data = split_line(line)
        else:
            data = []
        temp=HeapNode()
        temp.file_ptr=i
        temp.data=data
        heap.append(temp)

    heapq.heapify(heap)

    opFilePtr = open(output_file, 'w')
    num_tempFiles = len(temp_filenames)
    fileRead=0
    num_records=0
    while(fileRead!=num_tempFiles):
        top = heapq.heappop(heap)
        final_data = []
        final_data.append(top.data)
        writeToFile(opFilePtr, final_data)
        num_records+=1
        line = filepointer[top.file_ptr].readline()
        if line :
            new_data = split_line(line)
        else:
            new_data = []
        
        if(len(new_data)!=0):
            top.data = new_data
            heap.append(top)
            heapq.heapify(heap)
        else:
            # print("file read : ", fileRead)
            fileRead+=1
    
    print("all files done : ", num_records)
    for i in range(len(temp_filenames)):
         filepointer[i].close()
    
    opFilePtr.close()
    return         


def writeToFile(fptr, data):
    for line in data:
        temp=""
        for word in line:
            temp += (word + "  ")
        temp = temp[:-2]
        temp+="\r\n"
        fptr.write(temp)
        


def sortOn(x, sort_acc_to_cols):
    res = []
    for col in sort_acc_to_cols:
        res.append(x[col_index[col]])
    return res

def getSortedData(data, order_flag, sort_acc_to_cols):
    return sorted(data, key=lambda x : sortOn(x, sort_acc_to_cols), reverse = order_flag)

def split_line(line):
    len_of_cols = col_isto_size.values()
    start_index=0
    words = []

    for col_len in len_of_cols:
        words.append(line[start_index:start_index+col_len])
        start_index = start_index+col_len+2

    return words


def split_sort_storefile(input_file, order_flag, chunksize, sort_acc_to_cols):
    input_f = open(input_file)
    input_file_line = input_f.readline()

    temp_filenames = []
    temp_data = []
    tempfile_index = 0
    num_lines = 0

    while(input_file_line):
        words = split_line(input_file_line)
        temp_data.append(words)
        input_file_line = input_f.readline()
        num_lines+=1

        if(num_lines==chunksize):
            temp_filename = str(tempfile_index)+'.txt'
            # print ("Creating temp file : ", temp_filename)
            temp_filenames.append(temp_filename)
            tempfile = open(temp_filename, 'w')
            sorted_tempdata = getSortedData(temp_data, order_flag, sort_acc_to_cols)
            writeToFile(tempfile, sorted_tempdata)
            temp_data= []
            tempfile_index+=1
            num_lines=0
            tempfile.close()

    if(num_lines > 0):
        temp_filename = str(tempfile_index)+'.txt'
        # print ("Creating temp file : ", temp_filename)
        temp_filenames.append(temp_filename)
        tempfile = open(temp_filename, 'w')
        sorted_tempdata = getSortedData(temp_data, order_flag, sort_acc_to_cols)
        writeToFile(tempfile, sorted_tempdata)
        temp_data= []
        tempfile_index+=1
        num_lines=0
        tempfile.close()

    input_f.close()
    return temp_filenames


def getTupleSize():
    col_size = col_isto_size.values()
    num_cols = len(col_size)
    return sum(col_size)+(num_cols-1)*2+2

def getTotalNumOfRecords(filename):
    f = open(filename)
    lines = f.readlines()
    f.close()
    return len(lines)

def read_metadata(metadata_filename):
    f = open(metadata_filename)
    lines = f.readlines()
    f.close()

    for i in range(len(lines)):
        col, size = lines[i].split(',')
        col_isto_size[col] = int(size)
        col_index[col] = i


def parseInput(sys_args):
    input_file = sys_args[1]
    output_file = sys_args[2]
    mm_size = int(sys_args[3])*1000*1000
    num_thread = int(sys_args[4])
    order = sys_args[5]
    if(order=="desc"):
        order_flag = True
    elif (order=="asc"):
        order_flag = False
    else:
        print ("incorrect sorting order")
        sys.exit()
    
    col_to_sort = []
    for i in range(6, len(sys_args)):
        col_to_sort.append(sys_args[i])
    
    return input_file, output_file, mm_size, num_thread, order_flag, col_to_sort

def main():
    start_time = time.time()
    sys_args = sys.argv
    len_args = len(sys_args)
    metadata_filename = "metadata.txt"
    if len_args < 6:
        print ("Incomplete command")
    else:
        input_file, output_file, mm_size, num_thread, order_flag, sort_acc_to_cols = parseInput(sys_args)
        # print ("sort acc to col : ,", sort_acc_to_cols)
        read_metadata(metadata_filename)
        total_no_records = getTotalNumOfRecords(input_file)
        # print ("total num of records : ", total_no_records)
        tuple_size = getTupleSize()
        # print ("tuple_size : ", tuple_size)
        # chunksize = num of records in one chunk
        chunk_size = math.floor(mm_size/tuple_size)
        # print ("total num of temp files : ", math.ceil(total_no_records/chunk_size))
        # print ("chunk size : ", chunk_size)
        # print ("col_isto_size : ", col_isto_size)
        # print ("col_index : ", col_index)

        temp_filenames = split_sort_storefile(input_file, order_flag, chunk_size, sort_acc_to_cols)
        # print("temp files list : ", temp_filenames)

        res_len = checkRec(temp_filenames)
        # print ("mnum of rec in each dfile : ", res_len)
        # print ("total : ", sum(res_len))

        
        mergeFiles(temp_filenames, output_file, sort_acc_to_cols, order_flag)

        fileop = open(output_file)
        lines = fileop.readlines()
        # print ("num of records in op file : ", len(lines))
        fileop.close()
        # print (total_no_records*tuple_size/(1024*1024))
        delete_temp_files(temp_filenames)
        print ("time taken : ", time.time()-start_time)

main()

# command : python phase-1_0.py input.txt output.txt 100 5 desc A B

