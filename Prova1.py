import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta

def diff(t_a, t_b):
    t_diff = relativedelta(t_b, t_a)  # later/end time comes first!
    return '{h}h {m}m {s}s'.format(h=t_diff.hours, m=t_diff.minutes, s=t_diff.seconds)

def createbucket(nome):
    client = boto3.resource('s3')
    try:
        response = client.create_bucket(
            ACL='public-read-write',
            Bucket=nome,
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-1'
            },
        )
        print("Creato un nuovo bucket")
    except:
        print("Bucket già esistente")


def uploadfile(buckname):
    client = boto3.resource('s3')

    try:
        data = open('/home/daniel/Scrivania/TESI/video/240p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='240p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/360p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='360p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/720p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='720p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/1080p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='1080p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/v240p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='v240p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/v360p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='v360p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/v720p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='v720p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/v1080p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='v1080p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/vv240p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='vv240p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/vv360p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='vv360p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/vv720p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='vv720p.mp4', Body=data)

        data = open('/home/daniel/Scrivania/TESI/video/vv1080p.mp4', 'rb')
        client.Bucket(buckname).put_object(Key='vv1080p.mp4', Body=data)
    except:
        print('Già presenti')


def usingSNS(name):
    client = boto3.client('sns')
    response = client.create_topic(
        Name=name
    )
    return response


def usingstartlabeldetection(buckname, SNSTopicArn,name):
    client = boto3.client('rekognition')
    response = client.start_label_detection(
        Video={
            'S3Object': {
                'Bucket': buckname,
                'Name': name
            }
        },
        #ClientRequestToken='string',
        NotificationChannel={
            'SNSTopicArn': SNSTopicArn,
            'RoleArn': 'arn:aws:iam::136060013082:role/RuoloRekognition'
        },
        #JobTag='string'
    )
    return response['JobId']


def usingGetLabel(JobId):
    client = boto3.client('rekognition')
    response = client.get_label_detection(
        JobId=JobId,
        MaxResults=1000
    )
    return response

def anotherresult(JobId,NextToken):
    client = boto3.client('rekognition')
    response = client.get_label_detection(
        JobId=JobId,
        NextToken=NextToken,
        MaxResults=1000
    )
    return response


def scritturasufile(nome, label):
    file=open(nome, "w")
    file.write(str(label))
    file.close()


lista = ['240p', '360p', '720p', '1080p', 'v240p', 'v360p', 'v720p', 'v1080p', 'vv240p', 'vv360p', 'vv720p', 'vv1080p']
#lista = ['1']
#list = ['Long.mp4', 'Medium.mp4', 'Short.mp4', 'Small.mp4', 'Large.mp4']
mp4 = '.mp4'
txt = '.txt'

buckname = input("inserisci il nome del bucket:")

# Creo il bucket
createbucket(buckname)

# Carico i file nel bucket
uploadfile(buckname)

for i in range(0, len(lista)):
    # Creo un topic SNS per buttare i risultati dentro
    TopicArn = usingSNS(lista[i])
    print("TopicArn['TopicArn'] ", TopicArn['TopicArn'])
    # Rekognition start_label_detection
    JobId = usingstartlabeldetection(buckname, TopicArn['TopicArn'], lista[i]+mp4)
    print("JobID:", JobId)
    # Controllo con rekognition get label se lo status in SNS è SUCCEEDED

    start = datetime.now()
    label = usingGetLabel(JobId)
    while label['JobStatus'] != 'SUCCEEDED':
        label = usingGetLabel(JobId)
    risultato = str(label)
    while label.get('NextToken', 0) != 0:
        label = anotherresult(JobId, label['NextToken'])
        risultato += str(label)
    end = datetime.now()
    tempo = diff(end, start)
    scritturasufile(lista[i]+txt, risultato)

