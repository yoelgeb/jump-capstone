import boto3
import pandas as pd
import os

s3_resoucre = boto3.resource('s3')
s3_client = boto3.client('s3')

userinput = pd.DataFrame(columns=['price', 'brand', 'model', 'year', 'title_status', 'mileage', 'color', 'vin', 'state', 'hours_left'])

def frontend(userinput):
    carbrand = input('Please the brand name: ')
    carmodel = input('Please enter the model of the car: ')
    cartitle = input('Please enter the status of title: ')
    carcolor = input('Please enter the color of the car: ')
    while True:
        vinnumber = input('Please enter the Vin number of the car(17 digits only): ').upper()
        if (len(vinnumber) != 17):
            print("Please Enter exactly 17 digit vin number")
            continue
        elif (len(vinnumber) == 17):
            break
    auctionstate = input('Please input the state your listing at: ').title()
    while True:
            carprice = input('What is the car starting price: ')       
            try:
                cprice = float(carprice)
                if cprice < 1.0:
                    print("Please input a positive decimal number! ")
                    continue
            except ValueError:
                    print("Please enter a valid decimal number. ")
                    continue
            break 
    while True: 
            manufactureyear = input('What year was the car built in: ')       
            try:
                my = int(manufactureyear)
                if my < 1:
                    print("Please input a positive integer number! ")
                    continue
            except ValueError:
                    print("Please enter a valid integear number. ")
                    continue
            break 
    while True:
            carmiles = input('How many miles does the car have: ')
            try:
                cm = int(carmiles)
                if cm < 1:
                    print("Please input a positive integer number! ")
                    continue
            except ValueError:
                    print("Please enter a valid integer number. ")
                    continue
            break
    while True:        
            carauctiontime = int(input('Please enter how many hours do you want the car to be auctioned for: '))
            try:
                auctintime = int(carauctiontime)
                if auctintime < 1:
                    print("Please input a positive integer number! ")
                    continue
            except ValueError:
                    print("Please enter a valid integear number. ")
                    continue
            break
    
    userdataintodic = {
        'price': [cprice],
        'brand': [carbrand],
        'model': [carmodel], 
        'year': [my], 
        'title_status': [cartitle], 
        'mileage': [cm], 
        'color': [carcolor], 
        'vin': [vinnumber], 
        'state': [auctionstate], 
        'hours_left': [auctintime]
    }

    new = pd.DataFrame.from_dict(userdataintodic, orient='columns')
    newdataframe = pd.concat([new, userinput], ignore_index=True)
    return newdataframe
    
    
def inputDataWithCSV():
    user_input_file_path = input('Enter the exact path of your file: ')

    assert os.path.exists(user_input_file_path), 'I did not find the file at, ' +str(user_input_file_path)
    s3_resoucre.Bucket('jump-python-append').put_object(Key=f'from_upload-{user_input_file_path}', Body=open(user_input_file_path, 'rb'))

def uploadRawCarData():
    user_input_file_path = input('Enter the exact path of your file: ')
    s3_resoucre.Bucket('jump-python-capstone').put_object(Key='raw_data_upload-user-input.csv', Body=open(user_input_file_path, 'rb'))

while True:
    print("""-----------------------------
    Welcome To the Front End API
    (1). Add data manually
    (2). Add data with CSV
    (3). Upload the Raw Car Data
    (4). Exit
    
----------------------------- """)
        
    while True:
        ui = input()        
        try:
            menuinput = int(ui)
            if menuinput < 1:
                print("Please input a positive integer! ")
                continue
            elif menuinput > 3:
                print("Please input a integer (1 - 3)! ")
                continue
            
        except ValueError:
                print("Please enter a valid integer. ")
                continue
        break 

    while True:
        if menuinput == 1:
            userinput = frontend(userinput)
            print(userinput.head(10))
            break
        elif menuinput == 2:
            inputDataWithCSV()
            exit()
        elif menuinput == 3:
            uploadRawCarData()
            exit()
        elif menuinput == 4:
            s3_resoucre.Bucket('jump-python-append').put_object(Key='manual_input-user-input.csv', Body=userinput.to_csv())
            exit()


