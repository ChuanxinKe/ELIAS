"""
This is a terminal decoration module.
It aims at building a fresher-friendly presentation of terminal outputs.
However it can also be used to replace some of the notes.
So it won't bring to many burdens to developers and lines to script.
The author also believes that it helps to debug by using from the beginning.
Let your terminal talking!!!

By the way, it takes your machine some time to do so.
>>>if you are a starter: 
       it won't slow your script too much! Don't worry!
   elif:
       I believe you will use it wisely.

Table of Contents

1. Path Configuration Setting when Transplanted
2. Mission Notice and Timer
3. Count in a Flush Manner
4. Print First 5 Items of dict, list, string, tuple, numpy.array

Copyright: kcxwdzx@sina.com                    Last Modify Date:14/05/2020

"""
#Import--------------
import timeit
import time

# Path Configuration Setting when Transplanted-------------
class setpath:
    #You have to set path to create one.
    def __init__(self, path):
        self.path=path  
    def askupdate(self,description='Current path'):
        #Just put new file name inside new path.
        #description is optional, default as:'Current path'
        while True:
            print(description+': '+ self.path)
            change = input("Do you want to change? (y/n): ")
            if change == 'y':
                self.path = input('Please put new path here:')
                print ("\nNew path is set as: " + self.path + "\nThank You!\n")
                break
            elif change == 'n':
                print ("\nThis path is kept.Thank You!\n")
                break
            else:
                print('\nONLY y or n is accepted. Please try again.')

# Mission Notice and Timer----------------
class Mission:
    # begintime is used to get how many seconds are used so far.
    begintime = timeit.default_timer()
    def __init__(self, description=""):
        # description is optional for mission name. Default as:""
        self.description = description
        self.starttime = timeit.default_timer()
        print('\n>>>> MISSION: '+ self.description +' >>>>\n')   
    def end(self):
        # print the time this mission used and so far since initial.
        self.endtime = timeit.default_timer()
        interval = format(self.endtime-self.starttime,'.5f')
        sofar = format(self.endtime-self.begintime,'.5f')
        print('\n<<<< End: '+ self.description + ' , USE: '+interval + ' , SO FAR: ' + sofar + ' S <<<<\n')

# Count in a Flush Manner--------------------
class counter:   
    def __init__(self, sleep=0.01, description='Count',number=0):
        # Default: sleep 0.01s each count, print'Count' and start from 0.
        self.sleep=sleep
        self.description=description
        self.number=number
    def flush(self):
        # Count the number and print it in a flush way by [end='\r'].
        # So it may be disturbed by other print.  
        self.number=self.number+1
        print(self.description+': '+str(self.number),end='\r')
        time.sleep(self.sleep)

# Print first 5 items of dict, list, string, tuple, pandas.series, numpy.ndarray---------
def printfive(results, description="the target data"):
    # For pandas.DataFrame the head() and tail() is enough
    # description is optional. Default as: "the target data"   
    print('\nprint 5 samples: ' + description)
    print(type(results))
    try:
        if type(results)==type({"he":1}):
            a=list(results.items())[:5]
            print(a)
            print('Notes: Although print as list, it did not change actually.')       
        elif len(results)<5:
            for i in range(len(results)):
                print(results[i])
        else:           
            for i in range(5):
                print(results[i])
    except:
        print('Please check data. Only dict, list, string, tuple, pandas.series, numpy.ndarray are support!')

#Internal Test-----------------------
if __name__== '__main__':
    mission0 = Mission('Internal Test')

    mission1=Mission('set path when changing files')
    path1 = setpath("C:/user/coda.csv")
    path2 = setpath('/file/output/dashboard.png')
    path1.askupdate('This path is for input data')
    path2.askupdate('This path is for out put figure')
    mission1.end()
    
    mission2 = Mission('loop 500 times')
    c=counter()
    for i in range(500):
        c.flush()
    mission2.end()

    mission3=Mission('present different data sample')
    sample1 = ('North','South','West','East')
    sample2 = 'hello'
    sample3 = [1,2,3,4,5,6,7]
    sample4 = {"David":"UK", "Mike":"US", "James":"Japan","Tim":"US","Robert":"US","Chris":"China"}
    sample5 = set(["London","New York","Manchster","Leeds","Loughborough","Surry"])
    printfive(sample1,'sample1-position(tuple,4 items)')
    printfive(sample2,'sample2-hello(string, 5 items)')
    printfive(sample3, 'sample3-number(list, 7 items)')
    printfive(sample4, 'sample4-homenation(dict, 6 items)')
    printfive(sample5,'sample5-city(set, error here)')
    printfive(sample5)
    mission3.end()

    mission0.end()