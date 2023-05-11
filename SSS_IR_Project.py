from threading import Thread
import string

def tokenize(main_dictionary,text,index):
    #create key value pairs as [key - value] in a dictionary i.e.
    #The format is dictionary[10]=['t':1]
    #so it wolud be [{0:['t':1],1:['a':1],2:['c':1]}]
    dictionary=dict()
    alphabhets_lowercase=[]
    alphabhets_uppercase=[]
    alphabhets_lowercase=string.ascii_lowercase
    alphabhets_uppercase=string.ascii_uppercase
    count=0
    #lowercase and uppercases are treated differently, the can also be treated as by simply convert all the
    #alphabets in either lower or upper case
    
   
    for letter in text:
            for letter_2 in alphabhets_lowercase:
                if letter==letter_2:
                    dictionary[count]=[letter,1]
            for letter_3 in alphabhets_uppercase:
                if letter==letter_3:
                    dictionary[count]=[letter,1]
            if letter not in alphabhets_uppercase or letter not in alphabhets_uppercase:
                dictionary[count]=[letter,1]
            
            count=count+1
            #print(count,"\n")

    try:
        main_dictionary[index]=dictionary
    except IndexError:
        print("va;",index)
    #return(dictionary)       

def custom_key(value):
    #use this funcion to sort nested list i.e. sort according to in [['t',2]]
    return value[0]  # first parameter denotes the alphabet

def shuffling(list_of_dict,return_list):
    new_dict=dict()
    count=0
    #print("listofDict",list_of_dict)
    dictionary={}
    #create a common dictionary
    index=0
    for i in list_of_dict:
        #append dictionaries to make one
        for r in i.keys():
            dictionary[index]=i[r]
            index=index+1
        #print("eiction",dictionary,"i",i,"\n")
    temp = list(dictionary.values())
    
    #sort the values [c,1][d,2] alphabetically
    temp.sort(key=custom_key)
    #print("tem",temp)
  
    # reassigning to keys
    res = dict(zip(dictionary, temp))
    #print("res is",res)

    

          
            
    #print("dictionaru is ",dictionary)
    
    try:
        return_list[0]=res
    except IndexError:
        print("error")


    
def combiner(main_dictionary,dic,index):

    #combine occurance of repeated word in a single split data
    dictionary=dict()
    index_val=dic.keys()
    count=0
    for i in range(len(index_val)):
        flag=1
        for p in dictionary.keys():
                if dic[i][0] in dictionary[p][0]:
                   flag=0
        
        if flag:
            value=dic[i][1]
            #add the repeated values
            for j in range(i+1,len(index_val)):
                if dic[i][0]==dic[j][0]:
                   value=value+1
           
            dictionary[count]=[dic[i][0],value]
            count=count+1  

    
    
    try:
        main_dictionary[index]=dictionary
    except IndexError:
        print("va;",index)
def recordReducer(file_list):

    #split data according to lines from various fle list supplied
    listLines=[]
    for file in file_list:
        open_file=open(file,"r")
        linesSplit=open_file.read().splitlines()
        #print(linesSplit)
        for i in linesSplit:
            listLines.append(i)
    print("The input splits are as follows")
    incre=0
    for u in listLines:
        
        incre=incre+1
        print("input split", incre," : ", end="")
        print(u)
    lines={}    
    for i in range(len(listLines)):
            lines[i]=listLines[i]
    #print(lines)
    return lines



def multipleCall(main_dictionary,text,index):
    #call tokenize and combiner functions
    tokenize(main_dictionary,text,index)
  
    combiner(main_dictionary,main_dictionary[index],index)


def Reducer(return_output,temp_val,index):
    li=[]
    v_sum=0
    #extract dictionary from variable
    diction=temp_val[0]
  
    value_s=0
    sm_li=[]
    #calculate sum of occurances
    for i in temp_val[0].keys():

       alphabet=diction[i][0]
       sm_li.append(diction[i][1])
       v_sum=v_sum+diction[i][1]
    # return output in 0={'c':5} format
    return_output[index]=[alphabet,v_sum]
        
        

def partioner(mainlist):
    list_dict=[]
    threads=[]
    main_list=[]
    #assign dictionary
    diction=mainlist[0]
    
    #threads, partion data
    for i in diction.keys():
        threads.append(None)
        main_list.append(None)
        
    value_itr=list(diction.keys())
    temp_val=[{}]
    count=1
    store_val=[]
    for i in range(len(value_itr)-1):
        #here compasrion is done with the previous element and if same then create one group else individual element to reducer
        #Group or single element passed to the reducer
            #temp_val[0]={i:diction[i]}
        temp_val[0].update({i:diction[i]})

        #stores previous element
        store_val.append(temp_val[0])
        
            
            #print("tmp val",temp_val)
##        else:
##            count=1
##            continue
        if store_val[0][i][0]==diction[i+1][0]:
            continue
        else:
            #print(temp_val)
            threads[i] = Thread(target=Reducer, args=(main_list,temp_val,i))
            threads[i].start()
            store_val=[]
            temp_val=[{}]

    #wair foe the threads to complete their task   
    for i in range(len(threads)):
        if threads[i]!=None:
            threads[i].join()
    return main_list

def merge(v_list):
    #merge output, remove None elements
    li=[]
    for i in v_list:
        if i!=None:
            li.append(i)
    return(li)
def mapper(lines):
    list_dict=[]
    threads=[]
    main_list=[]
    #threads for simultaeous mapping
    for i in lines.keys():
        threads.append(None)
        main_list.append(None)
    for i in lines.keys():
        threads[i] = Thread(target=multipleCall, args=(main_list,lines[i],i))
        threads[i].start()
    
    
    #finish all threads execution
    for i in range(len(threads)):
        threads[i].join()
    return_list=[None]
    #print("shuff;e",main_list)
    print("The output after mapping and combining is : ")
    for i in main_list:
        for t in i.keys():
            print(i[t])
    shuffling(main_list,return_list)
    print("The output after shuffling and sorting is : ")
    for i in main_list:
        for t in i.keys():
            print(i[t])
    #None lists for processes
    return_output=partioner(return_list)

    final_res=merge(return_output)
    print("The output after partioning and reducing and merging is : ")
    for i in final_res:
        print(i)

    return(final_res)



def mapReduceAlgorithm(file):
    #call recordReducer
    lines=recordReducer(file)
    
    final_ans=mapper(lines)

    return final_ans
    
    



file1="C:/Users/Admin/AppData/Local/Programs/Python/Python38/mapReduceText.txt"
file2="mapReduceText2.txt"
file="mapReduceText2.txt"

#Create list of all the files
fileList=[]
fileList.append(file1)
fileList.append(file2)

#call the mapreduce algorithm function
alphabet_count=mapReduceAlgorithm(fileList)
print("The Alphabet Count using the Map Reduce Algorithm is as follows: \n")
for i in alphabet_count:
    print(i)

