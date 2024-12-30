#!/usr/bin/env python3

import os, subprocess
from time import sleep

subprocess.run(["g++", "sqlite.cpp", "-o", "sqlite"])

print("Current directory:", os.getcwd())

sqlite = ["sh", "-c", "cc ./sqlite.cpp -o sqlite -lstdc++ && ./sqlite"]



proc = subprocess.Popen(sqlite, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=os.getcwd(), shell=False)
sleep(5)

EXIT_COMMAND = '.exit'

def welcome_check():
    s = get_output()
    print(f"1, {s}")
    if s == 'Welcome to sqlite':
        return True 
    return False

def run_command(command):
    proc.stdin.write(f'{command}\r\n'.encode(encoding='utf-8'))
    proc.stdin.flush()

def get_output():
    output = proc.communicate()
    return output[0].decode('utf-8')

def exit_test():

    run_command(EXIT_COMMAND)
    if 'byeee' in get_output():
        return True
    return False
    
def insert():
    run_command('insert 1 abc abc@gmail.com')


    
#welcome_check()
exit_test()