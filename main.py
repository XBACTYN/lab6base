from datetime import date
from sqlalchemy import create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer, Date, Table, ForeignKey, Boolean
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.orm import joinedload


#mysqlclient скачивал или как то так


Base = declarative_base()
engine =  create_engine('mysql+mysqldb://root:root@localhost/lab6base')
Session = sessionmaker(bind=engine)
session = Session()


association = Table('association',
                    Base.metadata,
                    Column('id_musician',Integer, ForeignKey('musician.id')),
                    Column('id_release',Integer, ForeignKey('release.id')))


class Musician(Base):
    __tablename__ = 'musician'

    id = Column(Integer, primary_key=True,autoincrement=True,nullable=False)
    name = Column(String(50))
    birthday = Column(Date)

    products = relationship('Release',secondary=association,backref='authors')
    manager = relationship('Manager',uselist=False,backref= 'client')
    contacts = relationship('ContactDetails',backref = 'owner')


class Release(Base):
    __tablename__ = 'release'
    id = Column(Integer,primary_key=True,autoincrement=True,nullable=False)
    name = Column(String(50))
    date = Column(Date)


class Manager(Base):
    __tablename__ = 'manager'

    id = Column(Integer,primary_key = True,autoincrement=True,nullable=False)
    name = Column(String(50))
    phone = Column(String(14))
    id_musician = Column(Integer,ForeignKey('musician.id'))


class ContactDetails(Base):
    __tablename__ = 'contact_details'
    id= Column(Integer,primary_key=True,autoincrement=True,nullable=False)
    phone = Column(String(14))
    city = Column(String(50))
    country = Column(String(50))
    id_owner = Column(Integer,ForeignKey('musician.id'))


def AddData(arr3):
    arr3[0].phone = '+83338760089'

    session.commit()

def ShowReleasesAfter(date):
    releases_after=[]
    print(f'\nreleases after {date} : \n')
    for el in session.query(Release).filter(Release.date > date).order_by(Release.date):
        print('Release: ',el.name,'   Date:',el.date)
        releases_after.append(el)
    print('\n\n')

    return releases_after

def ShowReleasesByPerson(name):
    arr=[]
    print(f'\n{name} releases:\n')
    tab=session.query(Release).join(Musician.products).filter(Musician.name == name)
    for el in tab:
        print('Release: ',el.name,'   Date:',el.date)
        arr.append(el)
    print('\n\n')
    return arr

def ShowRichMusicians():
    arr=[]
    print('\nrich:\n')
    tab=session.query(Musician).join(Musician.contacts).group_by(Musician.id).having(func.count()>1).all()
    for el in tab:
        print('Name',el.name)
        for e in el.contacts:
            print('phone: ',e.phone)
        arr.append(el)
    print('\n\n')
    #print(tab)
    return arr

def ShowSingles():
    arr=[]
    print('\nsingles: \n')
    tab = session.query(Manager.name,Manager.phone).join(Musician).join(association).group_by(association.columns.id_musician).having(func.count(association.columns.id_musician)==1).all()
    #for el in tab:
        #print(el.name)
    print(tab)

if __name__ == '__main__':

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    arr1 = []
    arr1.append(Musician(name='Michael Gonzales', birthday='1979-03-23'))
    arr1.append(Musician(name='Zepp Branigan', birthday='1985-04-16'))
    arr1.append(Musician(name='Чайковский Петр Ильич', birthday='1840-05-07'))
    arr1.append(Musician(name='Artis Leon Ivey', birthday='1963-07-01'))
    arr1.append(Musician(name='Till Lindemann', birthday='1963-01-04'))
    arr1.append(Musician(name='Шнуров Сергей Владимирович', birthday='1973-04-13'))

    session.add_all(arr1)
    session.commit()

    arr2 = []
    arr2.append(Release(name='Best year ever, 2012 forever', date='2012-12-12',authors=[arr1[0],arr1[3]]))
    arr2.append(Release(name='Futurama: Kiffs serenade', date='2003-04-04',authors=[arr1[1],]))
    arr2.append(Release(name='Славянский марш', date='1876-09-01',authors=[arr1[2],]))
    arr2.append(Release(name='1812 год(увертюра)', date='1882-08-08',authors=[arr1[2],arr1[5]]))
    arr2.append(Release(name='Gangstas Paradise', date='1995-11-7',authors=[arr1[3],]))
    arr2.append(Release(name='Rosenrot', date='2005-10-28',authors=[arr1[4],]))
    arr2.append(Release(name='Spieluhr', date='2002-10-14',authors=[arr1[4],]))
    arr2.append(Release(name='Mutter', date='2002-10-14',authors=[arr1[4],]))
    arr2.append(Release(name='Выборы,Выборы', date='2007-10-10',authors=[arr1[5],arr1[3]]))
    arr2.append(Release(name='Ноу ноу фьюче', date='2003-09-28',authors=[arr1[5],]))

    session.add_all(arr2)
    session.commit()

    arr3 = []
    arr3.append(Manager(name='Barry Block', phone='+51238760089', client=arr1[0]))
    arr3.append(Manager(name='Kiff', phone='+17776661234', client=arr1[1]))
    arr3.append(Manager(name='Пиарщик Всея Руси', phone='+7917513820', client=arr1[2]))
    arr3.append(Manager(name='Tommy Boy', phone='+10671248769', client=arr1[3]))
    arr3.append(Manager(name='Adolf Artist', phone='+491006704501', client=arr1[4]))
    arr3.append(Manager(name='Алексей Киров', phone='+79175699807', client=arr1[5]))

    session.add_all(arr3)
    session.commit()

    arr4 = []
    arr4.append(ContactDetails(phone='+51238760089', city='Mexico', country='Mexico', owner=arr1[0]))
    arr4.append(ContactDetails(phone='+91868741293', city='New New York', country='USA', owner=arr1[1]))
    arr4.append(ContactDetails(phone='+68451927490', city='Los Angeles', country='USA', owner=arr1[1]))
    arr4.append(ContactDetails(phone='+79250017539', city='Санкт-Петербург', country='Россия', owner=arr1[2]))
    arr4.append(ContactDetails(phone='+165912300503', city='Detroit', country='USA', owner=arr1[3]))
    arr4.append(ContactDetails(phone='+59784551743', city='Berlin', country='Germany', owner=arr1[4]))
    arr4.append(ContactDetails(phone='+169450085637', city='Frankfurt am Main', country='Germany', owner=arr1[4]))
    arr4.append(ContactDetails(phone='+89260041720', city='Санкт-Петербург', country='Россия', owner=arr1[5]))

    session.add_all(arr4)
    session.commit()

    ShowReleasesAfter('2003-01-01')
    ShowReleasesByPerson('Шнуров Сергей Владимирович')
    ShowRichMusicians()
    ShowSingles()