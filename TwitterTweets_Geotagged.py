# -*- coding: utf-8 -*-
import tweepy, sys, time, string, ConfigParser, os
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
import unicodecsv

class StdOutListener(StreamListener):

    def __init__(self, api = None):
        self.api = api or API()
        self.counter = 0
        self.aika = time.strftime('%Y%m%d-%H%M%S')

        self.nimi = "Test_Geotagged_TwitterData_" + self.aika + '.txt'
        self.outFolder = r'C:\HY-Data\HENTENKA\Twitter\Geotagged_2014_05'

        self.outPath = os.path.join(self.outFolder, self.nimi)
        self.Schema = os.path.join(self.outFolder,'schema.ini')

        self.output = open(self.outPath, 'w')

        self.out = unicodecsv.writer(self.output, encoding='utf-8', delimiter='\t')
        self.output.write("timestamp¤username¤userID¤lon¤lat¤text¤retweeted_cnt¤language¤followers_cnt¤timeZone\n")

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
                pass
            else:

                ketju = status.created_at,"¤",status.user.name,"¤",status.user.id_str,"¤",status.coordinates.values()[1][0],"¤",status.coordinates.values()[1][1],"¤",status.text,"¤",status.retweet_count,"¤",status.user.lang,"¤",status.user.followers_count,"¤",status.user.time_zone
                info = "".join(map(str, ketju))
                #print info + "\n"
                self.output.write(info + "\n")

                self.counter+=1

                if self.counter >= 2000000:
                    self.output.close()
                    self.aika = time.strftime('%Y%m%d-%H%M%S')
                    self.nimi = "Test_Geotagged_TwitterData_" + self.aika + '.txt'
                    
                    self.outPath = os.path.join(self.outFolder, self.nimi)
                                        
                    self.output = open(self.outPath, 'a')
                    self.output.write("timestamp¤username¤userID¤lon¤lat¤text¤retweeted_cnt¤language¤followers_cnt¤timeZone\n")
                    
                    self.config = ConfigParser.RawConfigParser()
                    self.config.add_section(self.nimi)
                    self.config.set(self.nimi, 'Format', 'Delimited(\xa4)')

                    with open(self.Schema, 'a') as configfile:
                        self.config.write(configfile)
                    
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

    def error_handler(e, wait_period):
        error = str(e)

        if 'IncompleteRead' in error:
            print "Got an 'IncompleteRead' exception.. Continuing.."
            time.sleep(1)
            return 2
        elif 'Rate limit exceeded' in error:
            #Wait for 15 minutes
            print "\n\n\nRate limit exceeded..Sleeping for 15 minutes..\n\n\n"
            time.sleep(60*15+3)
            print "\n\n\nTrying to continue streaming..\n\n\n"
            return 2
        
        elif 'Error 500' in error or 'Error 502' in error or 'Error 503' in error or 'Error 504' in error:
            #Twitter has some problems of its own
            #Wait for few seconds and try again
            
            if wait_period < 3600: #If wait period gets over an hour stop raise an error and stop
                print "Got an Twitter server related exception.. Continuing in %s seconds.." % wait_period
                
                time.sleep(wait_period)
                wait_period *= 1.5
                return wait_period
                
            else:
                print "Too many retries.. Quitting.."
                raise e
        else:
            print "Got a new exception.."
            print error
            print "Stopping now.."
            raise e

        
        
    
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
    
    while True:
        try:
            stream.filter(locations=searchArea) #, locations=[-180,-90,180,90])#(locations=[-180,-90,180,90]) #) track=["Obama"]locations=[-180,-90,180,90] track=["Arnold Schwarzenegger"] track=["Fukushima"]locations=[-180,-90,180,90]
        except Exception as e:
            error = str(e)

            if 'IncompleteRead' in error:
                print "Got an 'IncompleteRead' exception.. Continuing.."
                time.sleep(1)
                return 2
            elif 'Rate limit exceeded' in error:
                #Wait for 15 minutes
                print "\n\n\nRate limit exceeded..Sleeping for 15 minutes..\n\n\n"
                time.sleep(60*15+3)
                print "\n\n\nTrying to continue streaming..\n\n\n"
                return 2
            
            elif 'Error 500' in error or 'Error 502' in error or 'Error 503' in error or 'Error 504' in error:
                #Twitter has some problems of its own
                #Wait for few seconds and try again
                
                if wait_period < 3600: #If wait period gets over an hour stop raise an error and stop
                    print "Got an Twitter server related exception.. Continuing in %s seconds.." % wait_period
                    
                    time.sleep(wait_period)
                    wait_period *= 1.5
                    return wait_period
                    
                else:
                    print "Too many retries.. Quitting.."
                    raise e
            else:
                print "Got a new exception.."
                print error
                print "Stopping now.."
                raise e


            #wait_period = l.error_handler(e, wait_period)
                            
                        
                               

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0) 

    
    
