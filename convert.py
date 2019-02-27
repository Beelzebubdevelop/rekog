def letturadafile(nome):
    file = open(nome, "r")
    rfile = file.read()
    return rfile

def operationfile(nome, file, index):
    iniziotimestamp = file.find('Timestamp', index)
    finetimestamp = file.find(',', iniziotimestamp)
    scritturasufile(nome,'Timestamp: '+file[iniziotimestamp+12:finetimestamp])
    iniziolabel = file.find('Label',finetimestamp)
    finelabel= file.find(',',iniziolabel)
    scritturasufile(nome,'Label: '+file[iniziolabel+18:finelabel-1])
    inizioconfidence = file.find('Confidence',finelabel)
    fineconfidence = file.find(',',inizioconfidence)
    scritturasufile(nome, 'Confidence: '+file[inizioconfidence+12:fineconfidence])
    return fineconfidence

def scritturasufile(nome, label):
    file=open(nome, "a")
    if label.startswith('Timestamp'):
        file.write('\n' + label)
    else:
        file.write('\t' + label)
    file.close()



percorsoandata = '/home/daniel/PycharmProjects/Coverttxt/'
percorsorisposte = '/home/daniel/PycharmProjects/Coverttxt/risposte/'
lista = ['240p', '360p', '720p', '1080p', 'v240p', 'v360p', 'v720p', 'v1080p', 'vv240p', 'vv360p', 'vv720p', 'vv1080p']
txt = '.txt'



for i in range (0,len(lista)):
    risposta = letturadafile(percorsoandata+lista[i]+txt)
    indice = 0
    while indice != -1:
        indice = operationfile(percorsorisposte+lista[i]+txt, risposta, indice)
    print('Finito il', i)
print('Finito tutto')