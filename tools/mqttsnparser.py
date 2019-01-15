import pandas as pd
import argparse
import errno
import os

parser = argparse.ArgumentParser(description='Parse the arguments')
parser.add_argument('-f','--file_path',help='Define the path to find the csv file')
args = parser.parse_args()

def reorder(dataframe):
    dataframe = dataframe[['timestamp','packetLength','info','networkInterface.sourceMAC', 'networkInterface.destinationMAC',
                   'internet.sourceIP','internet.destinationIP','internet.protocol','TCP.sourcePort',
                   'TCP.destinationPort','TCP.flags','TCP.calculatedWindowSize','UDP.sourcePort','UDP.destinationPort',
                   'MQTT.flags','MQTT.message','MQTT.topic','MQTT.messageLength','MQTT.frameCounter']]
    return dataframe


def numtodic(strnum):
    nuevodic = {}
    if(str(strnum[0])==""):
        return ""
    nuevodic["MQTT.messageLength"] = int(strnum[0:2])
    if (str(strnum[2:4])=="00"):
        nuevodic["info"]="ADVERTISE"
    elif("01"==str(strnum[2:4])):
        nuevodic["info"]="SEARCHGW"
    elif("02"==str(strnum[2:4])):
        nuevodic["info"]="GWINFO"
    elif("04"==str(strnum[2:4])):
        nuevodic["MQTT.flags"]=str(strnum[4:6])
        nuevodic["info"]="TYPE:CONNECT;PROTOCOLID:"+str(strnum[6:10])+";DURATION:"+str(strnum[10:14])+";CLIENTID:"+str(strnum[14:len(strnum)])
    elif("05"==str(strnum[2:4])):
        nuevodic["info"]="CONNACK"
    elif("06"==str(strnum[2:4])):
        nuevodic["info"]="WILLTOPICREQ"
    elif("07"==str(strnum[2:4])):
        nuevodic["info"]="WILLTOPIC"
        nuevodic["MQTT.flags"]=str(strnum[4:6])
        nuevodic["info"]="TYPE:WILLTOPIC;WILLTOPIC:"+str(strnum[6:len(strnum)])
    elif("08"==str(strnum[2:4])):
        nuevodic["info"]="WILLMSGREQ"
    elif("09"==str(strnum[2:4])):
        nuevodic["info"]="WILLMSG"
    elif("0a"==str(strnum[2:4])):
        nuevodic["MQTT.topic"]=str(strnum[4:8])
        nuevodic["info"]="TYPE:REGISTER;MESSAGEID:"+str(strnum[8:12])+";TOPICNAME:"+str(bytes.fromhex(str(strnum[12:(len(strnum))])).decode('utf-8'))
    elif("0b"==str(strnum[2:4])):
        nuevodic["info"]="REGACK"
    elif("0c"==str(strnum[2:4])):
        nuevodic["MQTT.flags"]=str(strnum[4:6])
        nuevodic["MQTT.topic"]=str(strnum[6:10])
        nuevodic["info"]="TYPE:PUBLISH;MESSAGEID:"+str(strnum[10:14])
        nuevodic["MQTT.message"]=bytes.fromhex(str(strnum[14:(len(strnum))])).decode('utf-8')
    elif("0d"==str(strnum[2:4])):
        nuevodic["info"]="PUBACK"
    elif("0e"==str(strnum[2:4])):
        nuevodic["info"]="PUBCOMP"
    elif("0f"==str(strnum[2:4])):
        nuevodic["info"]="PUBREC"
    elif("10"==str(strnum[2:4])):
        nuevodic["info"]="PUBREL"
    elif("12"==str(strnum[2:4])):
        nuevodic["MQTT.flags"]=str(strnum[4:6])
        nuevodic["info"]="TYPE:SUBSCRIBE;MESSAGEID:"+str(strnum[6:10])
        nuevodic["MQTT.topic"]=str(strnum[10:(len(strnum))])
    elif("13"==str(strnum[2:4])):
        nuevodic["info"]="SUBACK"
    elif("14"==str(strnum[2:4])):
        nuevodic["info"]="UNSUBSCRIBE"
    elif("15"==str(strnum[2:4])):
        nuevodic["info"]="UNSUBACK"
    elif("16"==str(strnum[2:4])):
        nuevodic["info"]="TYPE:PINGREQ"+";CLIENTID:"+str(strnum[4:(len(strnum))])
    elif("17"==str(strnum[2:4])):
        nuevodic["info"]="PINGRESP"
    elif("18"==str(strnum[2:4])):
        nuevodic["info"]="TYPE:DISCONNECT"+";DURATION:"+str(strnum[4:8])
    elif("1a"==str(strnum[2:4])):
        nuevodic["info"]="WILLTOPICUPD"
    elif("1b"==str(strnum[2:4])):
        nuevodic["info"]="WILLTOPICRESP"
    elif("1c"==str(strnum[2:4])):
        nuevodic["info"]="WILLMSGUPD"
    elif("1d"==str(strnum[2:4])):
        nuevodic["info"]="WILLMSGRESP"
    elif("1e"==str(strnum[2:4])):
        nuevodic["MQTT.flags"]=str(strnum[4:6])
        nuevodic["info"]="TYPE:SUB_WILDCAR;MESSAGEID:"+tr(strnum[6:10])
        nuevodic["MQTT.topic"]=str(strnum[10:(len(strnum))])
    return nuevodic


if __name__ == '__main__':
    if(args.file_path is  None):
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filename)
    else:
        df_conv = pd.read_csv(args.file_path,error_bad_lines=False)
        df_conv['Data'] = df_conv["Data"].apply(lambda x: numtodic(str(x)))
        df_conv["MQTT.flags"]=df_conv["Data"].apply(lambda x: x["MQTT.flags"] if "MQTT.flags" in x else -1)
        df_conv["MQTT.message"]=df_conv["Data"].apply(lambda x: x["MQTT.message"] if "MQTT.message" in x else -1)
        df_conv["MQTT.messageLength"]=df_conv["Data"].apply(lambda x: x["MQTT.messageLength"] if "MQTT.message" in x else -1)
        df_conv["MQTT.topic"]=df_conv["Data"].apply(lambda x: x["MQTT.topic"] if "MQTT.topic" in x else -1)
        df_conv["info"]=df_conv["Data"].apply(lambda x: x["info"] if "info" in x else -1)
        df_conv=reorder(df_conv)
        df_conv.to_csv("parsed_file.csv", quoting=2, index=False)
