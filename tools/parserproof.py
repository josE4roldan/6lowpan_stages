
import pandas as pd




def reorder(dataframe):
    dataframe = dataframe[['timestamp','packetLength','info',
                   'sourceIP','destinationIP','protocol','Source Port','Destination Port',
                   'MQTT.flags','MQTT.message','MQTT.topic','MQTT.messageLength']]
    return dataframe
def redo6low(dataframe):
    for index, row in dataframe.iterrows():
        if(row["protocol"]=="6LoWPAN"):
            row["sourceIP"]="aaaa::212:7402:2:202"
            row["destinationIP"]="aaaa::1"
    return dataframe

def numtodic(strnum):
    nuevodic = {}
    if(str(strnum[0])=="-1"):
        return "-1"
    nuevodic["MQTT.messageLength"] = int(strnum[0:2],16)
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
        nuevodic["MQTT.message"]=bytes.fromhex(str(strnum[14:])).decode('utf-8')
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


df_conv = pd.read_csv("1motecsvbig.csv",error_bad_lines=False)


# In[94]:
for i in df_conv.iloc[136]["Data"][14:]:
    print(str(i))
