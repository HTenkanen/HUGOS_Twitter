# -*- coding: utf-8 -*-
import tweepy, sys, time, os
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


    def on_status(self, status):

        #print status.text

        try:
            # If coordinates does not exist or they are invalid: do not proceed
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

    consumer_key = 'XXXXXX'
    consumer_secret = 'XXXXXX'

    access_token = 'XXXXXX'
    access_token_secret = 'XXXXXX'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    #Twitter error codes:
    "https://dev.twitter.com/docs/streaming-apis/connecting"

    #Twitter API parameters
    "https://dev.twitter.com/docs/streaming-apis/parameters"
        
    l = StdOutListener(api)
    stream = Stream(auth, l)

    #World bounding box
    searchArea=[-180,-90,180,90]

    maxRetries = 20
    retries = 0

    print "Streaming started..."
    while True:
        try:
            stream.filter(locations=searchArea) #, locations=[-180,-90,180,90])#(locations=[-180,-90,180,90]) #) track=["Obama"]locations=[-180,-90,180,90] track=["Arnold Schwarzenegger"] track=["Fukushima"]locations=[-180,-90,180,90]
            retries = 0
        except Exception as e:
            error = str(e)

            # Handling of some of the errors that may occur
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


