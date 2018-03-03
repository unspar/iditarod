import sqlite3


import sqlalchemy as sqa
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, UniqueConstraint, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import datetime
import time

#these names are hardcoded as we've picked 4 randomly
RACE_CHECKPOINTS = {
    2017 :["Manley", "Ruby", "Huslia", "Kaltag"],
    2016 : ["Rainy Pass", "McGrath", "Ruby", 'Kaltag'],
    2015 : ["Manley Hot Springs", "Ruby", 'Huslia','Kaltag'],
    2014 : ["Rainy Pass", "McGrath", "Ruby", 'Kaltag'],
    2013 : ["Rainy Pass", "McGrath", "Shageluk", 'Kaltag'],
    2012 : ["Rainy Pass", "McGrath", "Ruby", 'Kainyg'],
    2011 : ["Skwentna", "Nikolai", "Iditarod", 'Grayling'],
    2010 : ["Skwentna", "Nikolai", "Cripple", 'Galena']
}

FINISH = 'Nome'

DB_LOCATION = 'sqlite:///iditarod.db'

Base = declarative_base()

class Racer(Base):
    __tablename__ = 'racer'
    name = Column(String(50), primary_key=True)
    woman = Column(Boolean)
    first_race = Column(ForeignKey("race.year"))

class Race(Base):
    __tablename__ = 'race'
    year = Column(Integer, primary_key=True)
    
class CheckpointCrossing(Base):
    __tablename__ = 'ckp_crossing'
    id = Column(Integer, primary_key=True)
    racer = Column(ForeignKey("racer.name"), nullable=False)
    race = Column(ForeignKey("race.year"), nullable=False)
    checkpoint = Column(String(50), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    UniqueConstraint('racer', 'race', 'checkpoint')



def score_pos(position):
    return max((10 - position), 0)


def score_finish_pos(position):  
    pos = position
    return max((5-pos)*3, 0)  + max(35-pos, 0)


def score_checkpoint(order_list, score_func):
    
    return list(
        map(
            lambda x: (x[0],  score_func(x[1]) ),
            zip(order_list, range(len(order_list))) 
        )
    )


    
    
def io_get_checkpoint_results(session, race, checkpoint):
    #TODO - Handle errors
    ret = session.query(CheckpointCrossing).filter(
        CheckpointCrossing.race==race,
        CheckpointCrossing.checkpoint==checkpoint
    ).order_by(CheckpointCrossing.timestamp)
    return list(map(lambda x: x.racer, ret ))


def io_score_race_women(session, race):
    #TODO - Handle errors
    ret = session.query(CheckpointCrossing).filter(
        CheckpointCrossing.race==race,
        CheckpointCrossing.checkpoint==FINISH, 
        CheckpointCrossing.racer.woman ==True
    ).order_by(CheckpointCrossing.timestamp)
    return list(map(lambda x: x.racer, ret ))


    
    
    
def merge_scores(racer_points):
    points = {}
    for point_list in racer_points:
        for score in point_list:
            if score[0] in points:
                points[score[0]] += score[1]
            else: 
                points[score[0]] = score[1]
                
    return points
            
            
def io_score_race(session, race):

  points = []
  for ckp in RACE_CHECKPOINTS[race]:
    order = io_get_checkpoint_results(session, race, ckp)
    score= list(score_checkpoint(order, score_pos))
    points.append(score)
  
  points.append(
      score_checkpoint(
        io_get_checkpoint_results(session, race, FINISH),
        score_finish_pos
       )
  )
  return merge_scores(points)
#score checkpoint 1
#score checkpoint 2
#score checkpoint 3
#score finish
    #victory points
    #women
    #rookies
    #red lantern
#merge scores
#calculate mean + st_dev for each racer
    
    
    
def io_score_all_races(session):
    races = session.query(Race).order_by(year)
    
    for race in races:
        pass
    #get races in database
    #for race in races:
 


def loadEngine(conn_string):
  '''
  attempts to load an engine from a file path
  '''
  return sqa.create_engine(conn_string)


def loadSession(engine):
  '''
  creates a session from an engine
  '''
  Session = sessionmaker(bind=engine)
  session = Session()
  return session

def ioGenDatabase(conn_string, overwrite=False):
  '''
  makes a database of iex ticker data
  so you don't need to go over the network to query it
  '''
  
  engine = loadEngine(conn_string)
  session = loadSession(engine)
  Base.metadata.create_all(engine)
  session.commit()

'''

order = range(100)


fin = score_checkpoint(order, score_finish_pos)
ck1 = score_checkpoint(order, score_pos)

c1 =  CheckpointCrossing(
        racer='joe',
        race = 2017,
        checkpoint = 'first',
        timestamp=datetime.datetime.now()
    )
time.sleep(1)
c2 = CheckpointCrossing(
        racer='fred',
        race = 2017,
        checkpoint = 'first',
        timestamp=datetime.datetime.now()
    )

#sess.add_all([Racer(name="joe"), Racer(name='fred')])
#sess.add_all([c1, c2])


query = sess.query(Racer)
res = io_get_checkpoint_results(sess, 2017, 'first')


# In[6]:


sess.add_all([Racer(name="alice", woman=True), Racer(name='fred', woman=False)])
query = sess.query(Racer)
for i in query:
    print(i.name)
    print(i.woman)
'''

