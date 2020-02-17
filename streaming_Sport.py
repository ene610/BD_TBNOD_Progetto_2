import json
import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import re
import requests
import numpy as np

url = 'http://localhost:5004/Sport/'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.1 pyspark-shell'

sc = SparkContext(appName = "Sport")
stopword_list = ["a","abbastanza","abbia","abbiamo","abbiano","abbiate","accidenti","ad","adesso","affinche","agl","agli","ahime","ahimã¨","ahimè","ai","al","alcuna","alcuni","alcuno","all","alla","alle","allo","allora","altre","altri","altrimenti","altro","altrove","altrui","anche","ancora","anni","anno","ansa","anticipo","assai","attesa","attraverso","avanti","avemmo","avendo","avente","aver","avere","averlo","avesse","avessero","avessi","avessimo","aveste","avesti","avete","aveva","avevamo","avevano","avevate","avevi","avevo","avrai","avranno","avrebbe","avrebbero","avrei","avremmo","avremo","avreste","avresti","avrete","avrà","avrò","avuta","avute","avuti","avuto","basta","ben","bene","benissimo","berlusconi","brava","bravo","buono","c","casa","caso","cento","certa","certe","certi","certo","che","chi","chicchessia","chiunque","ci","ciascuna","ciascuno","cima","cinque","cio","cioe","cioã¨","cioè","circa","citta","città","cittã","ciã²","ciò","co","codesta","codesti","codesto","cogli","coi","col","colei","coll","coloro","colui","come","cominci","comprare","comunque","con","concernente","conciliarsi","conclusione","consecutivi","consecutivo","consiglio","contro","cortesia","cos","cosa","cosi","cosã¬","così","cui","d","da","dagl","dagli","dai","dal","dall","dalla","dalle","dallo","dappertutto","davanti","degl","degli","dei","del","dell","della","delle","dello","dentro","detto","deve","devo","di","dice","dietro","dire","dirimpetto","diventa","diventare","diventato","dopo","doppio","dov","dove","dovra","dovrà","dovrã","dovunque","due","dunque","durante","e","ebbe","ebbero","ebbi","ecc","ecco","ed","effettivamente","egli","ella","entrambi","eppure","era","erano","eravamo","eravate","eri","ero","esempio","esse","essendo","esser","essere","essi","ex","fa","faccia","facciamo","facciano","facciate","faccio","facemmo","facendo","facesse","facessero","facessi","facessimo","faceste","facesti","faceva","facevamo","facevano","facevate","facevi","facevo","fai","fanno","farai","faranno","fare","farebbe","farebbero","farei","faremmo","faremo","fareste","faresti","farete","farà","farò","fatto","favore","fece","fecero","feci","fin","finalmente","finche","fine","fino","forse","forza","fosse","fossero","fossi","fossimo","foste","fosti","fra","frattempo","fu","fui","fummo","fuori","furono","futuro","generale","gente","gia","giacche","giorni","giorno","giu","già","giã","gli","gliela","gliele","glieli","glielo","gliene","governo","grande","grazie","gruppo","ha","haha","hai","hanno","ho","i","ie","ieri","il","improvviso","in","inc","indietro","infatti","inoltre","insieme","intanto","intorno","invece","io","l","la","lasciato","lato","lavoro","le","lei","li","lo","lontano","loro","lui","lungo","luogo","là","lã","ma","macche","magari","maggior","mai","male","malgrado","malissimo","mancanza","marche","me","medesimo","mediante","meglio","meno","mentre","mesi","mezzo","mi","mia","mie","miei","mila","miliardi","milioni","minimi","ministro","mio","modo","molta","molti","moltissimo","molto","momento","mondo","mosto","nazionale","ne","negl","negli","nei","nel","nell","nella","nelle","nello","nemmeno","neppure","nessun","nessuna","nessuno","niente","no","noi","nome","non","nondimeno","nonostante","nonsia","nostra","nostre","nostri","nostro","novanta","nove","nulla","nuovi","nuovo","o","od","oggi","ogni","ognuna","ognuno","oltre","oppure","ora","ore","osi","ossia","ottanta","otto","paese","parecchi","parecchie","parecchio","parte","partendo","peccato","peggio","per","perche","perchã¨","perchè","perché","percio","perciã²","perciò","perfino","pero","persino","persone","perã²","però","piedi","pieno","piglia","piu","piuttosto","piã¹","più","po","pochissimo","poco","poi","poiche","possa","possedere","posteriore","posto","potrebbe","preferibilmente","presa","press","prima","primo","principalmente","probabilmente","promesso","proprio","puo","pure","purtroppo","puã²","può","qua","qualche","qualcosa","qualcuna","qualcuno","quale","quali","qualunque","quando","quanta","quante","quanti","quanto","quantunque","quarto","quasi","quattro","quel","quella","quelle","quelli","quello","quest","questa","queste","questi","questo","qui","quindi","quinto","realmente","recente","recentemente","registrazione","relativo","riecco","rispetto","salvo","sara","sarai","saranno","sarebbe","sarebbero","sarei","saremmo","saremo","sareste","saresti","sarete","sarà","sarã","sarò","scola","scopo","scorso","se","secondo","seguente","seguito","sei","sembra","sembrare","sembrato","sembrava","sembri","sempre","senza","sette","si","sia","siamo","siano","siate","siete","sig","solito","solo","soltanto","sono","sopra","soprattutto","sotto","spesso","srl","sta","stai","stando","stanno","starai","staranno","starebbe","starebbero","starei","staremmo","staremo","stareste","staresti","starete","starà","starò","stata","state","stati","stato","stava","stavamo","stavano","stavate","stavi","stavo","stemmo","stessa","stesse","stessero","stessi","stessimo","stesso","steste","stesti","stette","stettero","stetti","stia","stiamo","stiano","stiate","sto","su","sua","subito","successivamente","successivo","sue","sugl","sugli","sui","sul","sull","sulla","sulle","sullo","suo","suoi","tale","tali","talvolta","tanto","te","tempo","terzo","th","ti","titolo","torino","tra","tranne","tre","trenta","triplo","troppo","trovato","tu","tua","tue","tuo","tuoi","tutta","tuttavia","tutte","tutti","tutto","uguali","ulteriore","ultimo","un","una","uno","uomo","va","vai","vale","vari","varia","varie","vario","verso","vi","via","vicino","visto","vita","voi","volta","volte","vostra","vostre","vostri","vostro","ã¨","è"]
stopword_regex = " a | abbastanza | abbia | abbiamo | abbiano | abbiate | accidenti | ad | adesso | affinche | agl | agli | ahime | ahimã¨ | ahimè | ai | al | alcuna | alcuni | alcuno | all | alla | alle | allo | allora | altre | altri | altrimenti | altro | altrove | altrui | anche | ancora | anni | anno | ansa | anticipo | assai | attesa | attraverso | avanti | avemmo | avendo | avente | aver | avere | averlo | avesse | avessero | avessi | avessimo | aveste | avesti | avete | aveva | avevamo | avevano | avevate | avevi | avevo | avrai | avranno | avrebbe | avrebbero | avrei | avremmo | avremo | avreste | avresti | avrete | avrà | avrò | avuta | avute | avuti | avuto | basta | ben | bene | benissimo | berlusconi | brava | bravo | buono | c | casa | caso | cento | certa | certe | certi | certo | che | chi | chicchessia | chiunque | ci | ciascuna | ciascuno | cima | cinque | cio | cioe | cioã¨ | cioè | circa | citta | città | cittã | ciã² | ciò | co | codesta | codesti | codesto | cogli | coi | col | colei | coll | coloro | colui | come | cominci | comprare | comunque | con | concernente | conciliarsi | conclusione | consecutivi | consecutivo | consiglio | contro | cortesia | cos | cosa | cosi | cosã¬ | così | cui | d | da | dagl | dagli | dai | dal | dall | dalla | dalle | dallo | dappertutto | davanti | degl | degli | dei | del | dell | della | delle | dello | dentro | detto | deve | devo | di | dice | dietro | dire | dirimpetto | diventa | diventare | diventato | dopo | doppio | dov | dove | dovra | dovrà | dovrã | dovunque | due | dunque | durante | e | ebbe | ebbero | ebbi | ecc | ecco | ed | effettivamente | egli | ella | entrambi | eppure | era | erano | eravamo | eravate | eri | ero | esempio | esse | essendo | esser | essere | essi | ex | fa | faccia | facciamo | facciano | facciate | faccio | facemmo | facendo | facesse | facessero | facessi | facessimo | faceste | facesti | faceva | facevamo | facevano | facevate | facevi | facevo | fai | fanno | farai | faranno | fare | farebbe | farebbero | farei | faremmo | faremo | fareste | faresti | farete | farà | farò | fatto | favore | fece | fecero | feci | fin | finalmente | finche | fine | fino | forse | forza | fosse | fossero | fossi | fossimo | foste | fosti | fra | frattempo | fu | fui | fummo | fuori | furono | futuro | generale | gente | gia | giacche | giorni | giorno | giu | già | giã | gli | gliela | gliele | glieli | glielo | gliene | governo | grande | grazie | gruppo | ha | haha | hai | hanno | ho | i | ie | ieri | il | improvviso | in | inc | indietro | infatti | inoltre | insieme | intanto | intorno | invece | io | l | la | lasciato | lato | lavoro | le | lei | li | lo | lontano | loro | lui | lungo | luogo | là | lã | ma | macche | magari | maggior | mai | male | malgrado | malissimo | mancanza | marche | me | medesimo | mediante | meglio | meno | mentre | mesi | mezzo | mi | mia | mie | miei | mila | miliardi | milioni | minimi | ministro | mio | modo | molta | molti | moltissimo | molto | momento | mondo | mosto | nazionale | ne | negl | negli | nei | nel | nell | nella | nelle | nello | nemmeno | neppure | nessun | nessuna | nessuno | niente | no | noi | nome | non | nondimeno | nonostante | nonsia | nostra | nostre | nostri | nostro | novanta | nove | nulla | nuovi | nuovo | o | od | oggi | ogni | ognuna | ognuno | oltre | oppure | ora | ore | osi | ossia | ottanta | otto | paese | parecchi | parecchie | parecchio | parte | partendo | peccato | peggio | per | perche | perchã¨ | perchè | perché | percio | perciã² | perciò | perfino | pero | persino | persone | perã² | però | piedi | pieno | piglia | piu | piuttosto | piã¹ | più | po | pochissimo | poco | poi | poiche | possa | possedere | posteriore | posto | potrebbe | preferibilmente | presa | press | prima | primo | principalmente | probabilmente | promesso | proprio | puo | pure | purtroppo | puã² | può | qua | qualche | qualcosa | qualcuna | qualcuno | quale | quali | qualunque | quando | quanta | quante | quanti | quanto | quantunque | quarto | quasi | quattro | quel | quella | quelle | quelli | quello | quest | questa | queste | questi | questo | qui | quindi | quinto | realmente | recente | recentemente | registrazione | relativo | riecco | rispetto | salvo | sara | sarai | saranno | sarebbe | sarebbero | sarei | saremmo | saremo | sareste | saresti | sarete | sarà | sarã | sarò | scola | scopo | scorso | se | secondo | seguente | seguito | sei | sembra | sembrare | sembrato | sembrava | sembri | sempre | senza | sette | si | sia | siamo | siano | siate | siete | sig | solito | solo | soltanto | sono | sopra | soprattutto | sotto | spesso | srl | sta | stai | stando | stanno | starai | staranno | starebbe | starebbero | starei | staremmo | staremo | stareste | staresti | starete | starà | starò | stata | state | stati | stato | stava | stavamo | stavano | stavate | stavi | stavo | stemmo | stessa | stesse | stessero | stessi | stessimo | stesso | steste | stesti | stette | stettero | stetti | stia | stiamo | stiano | stiate | sto | su | sua | subito | successivamente | successivo | sue | sugl | sugli | sui | sul | sull | sulla | sulle | sullo | suo | suoi | tale | tali | talvolta | tanto | te | tempo | terzo | th | ti | titolo | torino | tra | tranne | tre | trenta | triplo | troppo | trovato | tu | tua | tue | tuo | tuoi | tutta | tuttavia | tutte | tutti | tutto | uguali | ulteriore | ultimo | un | una | uno | uomo | va | vai | vale | vari | varia | varie | vario | verso | vi | via | vicino | visto | vita | voi | volta | volte | vostra | vostre | vostri | vostro | ã¨ | è "
ssc = StreamingContext(sc,10) # (sparkContext, batchDuration

#kafkaStream = KafkaUtils.createStream(ssc, 'localhost:9092', 'spark-streaming', {'numtest':1})
kafkaStream = KafkaUtils.createDirectStream(ssc, ['Sport'], {'metadata.broker.list': 'localhost:9092'})

#regex = ""
#for filtro in stopword_list:
#        regex += "| " + filtro + " "
#print(regex[1:])

#accedi a qualche cosa json.loads(x[1])["created_at"] #window duration , slide duration
rdd = kafkaStream.window(60*15,10)


def JobWordCount(rdd):

    def sendWithRequests(record):
        request_data = {"Word_Count": record[1]}
        print(request_data)
        response = requests.post(url + "update_word_counter", data=json.dumps(request_data))

    def sendDataHtml(x):
        x.foreach(sendWithRequests)

    def sendDataJson(record):
        json_element = {"word": record[0], "count": record[1]}
        return ("", [json_element])

    def reducerKey(recordA, recordB):
        return recordA + recordB

    global stopword_regex

    def filterWord(line):
        result = " " + line["text"].lower() + " "
        result = re.sub("https://t.co/[0-z]*"," ",result)
        result = re.sub("@[0-z]*"," ",result)
        result = re.sub(" [0-z] "," ",result)
        result = re.sub(r"[^\w]", " " , result)
        result = re.sub("  +"," ",result)
        return result

    lines = rdd.mapValues(lambda x : json.loads(x)).mapValues(filterWord).flatMap(lambda x: x[1].split(" "))\
        .map(lambda x : re.sub(stopword_regex,""," "+x+" ")).map(lambda x : re.sub(" ","",x)).filter(lambda x : x != "")

    words = lines.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    sorted = words.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    sorted = sorted.map(sendDataJson).reduceByKey(reducerKey)
    sorted = sorted.foreachRDD(sendDataHtml)

def JobAuthorFollowersCount(rdd):
    def authorFollowers(x):
        return (json.loads(x)["screen_name"],json.loads(x)["followers_count_author"])

    def toJson(x):
        return [{"author":x[0],"count_follower":x[1]}]

    def risistema(x,y):
        return x+y
    def aaaa(record):
        print(record)
        response = requests.post(url + "update_author_follower", data=json.dumps(record))

    def sendDataHtmlAuthorFollower(x):
        x.foreach(aaaa)

    lines = rdd.mapValues(authorFollowers).transform(lambda rdd: rdd.sortBy(lambda x: x[1][1], ascending=False)).mapValues(toJson)
    sorted =lines.reduceByKey(risistema).map(lambda x : {"AuthorFollowers":x[1]})
    sorted.foreachRDD(sendDataHtmlAuthorFollower)

def JobMostUsedHashtags(rdd):

    def retriveHashtags(list):
        listHashtags = ""
        for hashtag in list["entities"]["hashtags"]:
            listHashtags += ",#" + hashtag["text"].lower()
        return listHashtags[1:]

    def toJson(line):
        json_element = {"hashtag_text":line[0],"hashtag_count":line[1]}
        return ("",[json_element])

    def sendWithRequests(record):
        request_data = {"hashtags_Count": record[1]}
        print(request_data)
        response = requests.post(url + "update_hashtags_count", data=json.dumps(request_data))

    def sendDataHtml(x):
        x.foreach(sendWithRequests)

    lines = rdd \
        .mapValues(lambda x: json.loads(x)) \
        .mapValues(retriveHashtags) \
        .flatMap(lambda x: x[1].split(",")) \
        .filter(lambda x: x != "") \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: x + y)


    sorted = lines.transform(
        lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

    sorted = sorted.map(toJson).reduceByKey(lambda x,y : x+y)
    sorted.foreachRDD(sendDataHtml)


#funzioni
JobWordCount(rdd)
JobAuthorFollowersCount(rdd)
JobMostUsedHashtags(rdd)

#saveToMongo
from pymongo import MongoClient
client_mongo = MongoClient('mongodb://localhost:27017/')
mydb = client_mongo.mydatabase
mycol = mydb.streamingMostUsedWord_Musica
mycol.drop()
def saveOnMongoDB(lines):

    print(mycol.insert_one(lines))
    return lines

def dropMongo(x):
    mycol.drop()
    return x

#sorted.mapValues(dropMongo).map(saveOnMongoDB)

ssc.start()
ssc.awaitTermination()



