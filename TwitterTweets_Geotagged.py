# -*- coding: utf-8 -*-
import tweepy, sys, time, string, ConfigParser, os
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
import unicodecsv

class StdOutListener(StreamListener):

    def __init__(self, api = None):
        self.api = api or API()
        self.counter = 0
        self.aika = None
        self.nimi = None
        self.outFolder = None
        self.startNewFile()
        self.writeArcGisConf()


    def closeFile(self):
        self.output.close()

    def startNewFile(self):
        self.aika = time.strftime('%Y%m%d-%H%M%S')

        self.nimi = "Extended_Geotagged_TwitterData_" + self.aika + '.txt'
        self.outFolder = r'C:\HY-Data\HENTENKA\Twitter\Geotagged_2014_05'

        self.outPath = os.path.join(self.outFolder, self.nimi)
        self.Schema = os.path.join(self.outFolder,'schema.ini')

        # Open output stream
        self.output = open(self.outPath, 'w')

        # Set up unicode writer
        self.out = unicodecsv.writer(self.output, encoding='utf-8', delimiter='\t')

        # Write header
        self.out.writerow(("timestamp","utc_offset","username", "userID", "description", "lon", "lat", "text", "reply_to_tweet", "reply_to_user", "retweeted_cnt", "favorite_cnt", "language", "followers_cnt", "friends_cnt", "tweets_cnt", "timeZone", "location", "place"))

    def writeArcGisConf(self):
        self.config = ConfigParser.RawConfigParser()
        self.config.add_section(self.nimi)
        self.config.set(self.nimi, 'Format', 'Delimited(\xa4)')

        with open(self.Schema, 'a') as configfile:
            self.config.write(configfile)
                   
    def on_status(self, status):

        #print status.text

        try:
            if status.coordinates is None or int(status.coordinates.values()[1][0]) == 0 and int(status.coordinates.values()[1][1]) == 0:
                pass
            if "\n" in status.text:
                status.text = status.text.replace('\n', '')
            if "\n" in status.user.description:
                status.user.description = status.user.description.replace('\n', '')
            if "\n" in status.user.location:
                status.user.location = status.user.location.replace('\n', '')

            else:

                self.out.writerow((status.created_at,status.user.utc_offset,
                                      status.user.name,status.user.id_str,status.user.description,
                                      status.coordinates.values()[1][0],status.coordinates.values()[1][1],status.text,
                                      status.in_reply_to_status_id_str,status.in_reply_to_user_id_str,status.retweet_count,status.favorite_count,
                                      status.user.lang,status.user.followers_count,status.user.friends_count,status.user.statuses_count,
                                      status.user.time_zone,status.user.location,status.place.name))

                self.counter+=1

                if self.counter >= 2000000:
                    self.closeFile()
                    self.startNewFile()
                    self.writeArcGisConf()
                    self.counter = 0
                                
        except:
            pass
        
          
        return True
        
    def on_error(self, status_code):
        print 'Error: ' + repr(status_code)
        return False

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream


def main():

    consumer_key = '4Fm2aXPRXvvqcR2ZUnnhYw'
    consumer_secret = 'VfbuyPM4Db7iarqH6iqS9khADXzRMyHFi4Z4FIn9nw'

    access_token = '1524365196-Hz3FIwqhIGoZ5tEUDLGay5cPEiIZ0znUEOs2Az8'
    access_token_secret = 'BaH2OFTJYfwsRv96GGpgARH1RpKXS38FpzopQpEqc'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    #Twitter error codes:
    "https://dev.twitter.com/docs/streaming-apis/connecting"

    #Twitter API parameters
    "https://dev.twitter.com/docs/streaming-apis/parameters"
        
    l = StdOutListener(api)
    stream = Stream(auth, l)

    #World:
    searchArea=[-180,-90,180,90]

    #Europe:
    #searchArea=[-13,31,40,83]

    #North Europe:    
    #searchArea=[3.0,54.0,34.0,72.5]
    
    
    keys=['national park,parque nacional,parco nazionale,parc nationa,kansallispuisto,национальный парк,заповедник,nationaal park,εθνικό πάρκο,nasjonalparken,milli park,park narodowy,parque naci,nationa park,národní park,nationa parku,国立公園,राष्ट्रीय उद्यान,rahvuspargi,Yellowstone,Kruger,Machu Picchu,Serengeti,Sagarmatha,Fiordland,Galapagos,Kakadu National park,Swiss national park,Guilin park,Lijiang river,Yosemite,Suomenlinna,Manuel Antonio,Lake district,Torres del Paine,Masai Mara,Banff park,Grand canyon,Yellowstone,Kruger,Machu Picchu,Serengeti,Sagarmatha,Fiordland,Galapagos Island,Kakadu National park,Swiss national park,Guilin national park,Yosemite,Manuel Antonio,Lake district,Torres del Paine,Masai Mara,Banff park,Grand canyon,Ngorongoro,Kilimanjaro,Tsavo,Amboseli,Etosha,Hwange,Moremi,Chobe,Okavango,Kalahari,Kidepo valley,Mantadia,Ahaggar,South Luangwa,Bwindi Impenetrable Forest,Virunga,Table Mountain,Addo Elephant Park,Hluhluwe iMfoloziPark,iSimangaliso Wetland Park,Niassa National Reserve, safari africa,safari nature,big5 nature,big5 africa, bigfive,elephant,lion,leopard,gorilla,tiger,rhino,zebra,gazelle,hippopotamus,crocodile,alligator,gnu,antelope,hyena,warthog,chimpanzee,cheetah,birdwatching,mammal']
                
    print "Streaming started..."

    
    wait_period = 2 #in seconds
    maxRetries = 20
    retries = 0
    
    while True:
        try:
            stream.filter(locations=searchArea) #, locations=[-180,-90,180,90])#(locations=[-180,-90,180,90]) #) track=["Obama"]locations=[-180,-90,180,90] track=["Arnold Schwarzenegger"] track=["Fukushima"]locations=[-180,-90,180,90]
            retries = 0
        except Exception as e:
            error = str(e)

            if 'IncompleteRead' in error:
                print "Got an 'IncompleteRead' exception.. Continuing.."
                time.sleep(1)
                pass
            elif 'Rate limit exceeded' in error:
                #Wait for 15 minutes
                print "\n\n\nRate limit exceeded..Sleeping for 15 minutes..\n\n\n"
                time.sleep(60*15+3)
                print "\n\n\nTrying to continue streaming..\n\n\n"
                pass

            elif 'Expecting value:' in error:
                print "Got an 'JSONDecodeError'.. Continuing.."
                time.sleep(1)
                pass
            elif "Unterminated string starting at" in error:
                print "Got an 'JSONDecodeError'.. Continuing.."
                time.sleep(1)
                pass
            elif "The read operation timed out" in error:
                print "The read operation timed out.. Continuing.."
                time.sleep(2)
                pass
            elif "Expecting property name enclosed in double quotes" in error:
                time.sleep(1)
                pass
            elif "getaddrinfo failed" in error:
                time.sleep(2)
                pass
            
            elif 'Error 500' in error or 'Error 502' in error or 'Error 503' in error or 'Error 504' in error:
                #Twitter has some problems of its own
                #Wait for few seconds and try again
                time.sleep(2)
                pass
            else:
                print "Got a new exception.."
                print error

                retries+=1
                if retries <= maxRetries: #If wait period gets over an hour stop raise an error and stop
                    print "Amount of retries so far: %s/%s" % (retries, maxRetries)
                    pass
                                
                else:
                    print "Too many retries.. Quitting.."
                    raise error
                
           

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0) 

    
    
