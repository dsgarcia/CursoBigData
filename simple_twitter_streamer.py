#!/usr/bin/python
# -*- coding: utf8 -*-

__author__      = "Daniel Sebastián García Parra"
"""extactor.py: Simple Twitter streamer, developed for Big Data course, Facultad de Ingenieria, Universidad de la Republica"""


import traceback
import sys
import signal
import logging
from requests.exceptions import ChunkedEncodingError
from twython import TwythonStreamer


class SimpleTwitterStreamer(TwythonStreamer):
  def __init__(self):
    self.name = 'tweets_collector'
    self.count = 1
    # logger
    self.logger = logging.getLogger(self.name)
    self.logger.debug('Initializing module.')
    # Twython
    app_key = ''
    app_secret = ''
    access_token = ''
    access_token_secret = ''

    #add handlers
    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGQUIT]:  #signal.SIGHUP no importa si se cierra la terminal
      signal.signal(sig, self.handler)

    #self.rest_twitter = Twython(self.appkeys.api_key.app_key, access_token=self.appkeys.access_token)
    TwythonStreamer.__init__(self, app_key, app_secret, access_token, access_token_secret)


  def handler(self, signum=None, frame=None):
    msg = "Signal handler called with signal: " + str(signum)
    print msg
    self.logger.debug(msg)
    # self.session.rollback()
    # self.session.close()
    #self.logger.debug("Sending email alert...")
    #analytics_mail_alerts.iDathaMailAlerts.sendMailAlert(msg,self.name)
    self.logger.debug("Exiting now...")
    sys.exit(0)

  # STREAMER EVENTS
  def on_success(self, data):
    if 'text' in data:
      # enchufar con Storm
      self.logger.debug('Tweets: ' + str(self.count) + ", id: " + str(data['id']) + ': ' + data['text'].encode('utf-8'))
      self.count += 1
    elif 'delete' in data:
      self.logger.debug( 'DELETION NOTICE: ' + str(data).encode('utf-8'))
      # TODO: delete status!
    elif 'warning' in data:
      self.logger.debug('STALL WARNING: ' + str(data).encode('utf-8'))
      # self.saveStallWarning(data)
    elif 'limit' in data:
      self.logger.debug('LIMIT NOTICE: ' + str(data).encode('utf-8'))
      # self.saveLimitNotice(data)
    elif 'disconnect' in data:
      self.logger.debug('DISCONNECTION MESSAGE: ' + str(data).encode('utf-8'))
      # TODO: save this?
    elif 'status_withheld' in data:
      self.logger.debug('STATUS WITHHELD ' + str(data).encode('utf-8'))
      # TODO: withhold status
    elif 'user_withheld' in data:
      self.logger.debug('USER WITHHELD: ' + str(data).encode('utf-8'))
      # TODO: withhold user
    else:
      self.logger.debug('Oh my god...: ' + str(self.count) + ', Data: ' + str(data))

  def on_error(self, status_code, data):
    msg = "Unexpected error in execution of the Twitter streamer process (status_code: " + str(
      status_code) + "). " + str(data)
    print msg
    print traceback.format_exc()
    print "Disconnecting..."
    self.disconnect()
    # self.logger.debug("Sending email alert...")
    # analytics_mail_alerts.iDathaMailAlerts.sendMailAlert(msg,self.name)
    print "Exiting now..."
    sys.exit(0)

  # END STREAMER EVENTS

  def run(self):
    follow_ids = ''
    track_terms = 'mujica'
    while True:
      try:
        if len(track_terms):
          if len(follow_ids):
            self.statuses.filter(track=track_terms, follow=follow_ids, stall_warnings=True)
          else:
            self.statuses.filter(track=track_terms, stall_warnings=True)
        else:
          self.statuses.filter(follow=follow_ids, stall_warnings=True)
      except ChunkedEncodingError as e:
        msg = "ChunkedEncodingError in execution of the search track processor. " + str(e)
        self.logger.debug(msg)
        self.logger.debug(traceback.format_exc())
        # self.logger.debug("Sending email alert...")
        # analytics_mail_alerts.iDathaMailAlerts.sendMailAlert(msg,self.name)
        self.logger.debug("Resetting streamer...")
      except Exception as e:
        msg = "Unexpected error in execution of the search track processor. " + str(e)
        self.logger.debug(msg)
        self.logger.debug(traceback.format_exc())
        # self.logger.debug("Sending email alert...")
        # analytics_mail_alerts.iDathaMailAlerts.sendMailAlert(msg,self.name)
        self.logger.debug("Exiting now...")
        sys.exit(0)


def main():
  print 'Process start...'
  processor = SimpleTwitterStreamer()
  processor.run()
  print 'Exiting now.'


if __name__ == "__main__":
  main()
