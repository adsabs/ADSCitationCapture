from sqlalchemy import Column, DateTime, String, Integer, func
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

#class RawCitation(Base):
    #__tablename__ = 'raw_citation'
    #__table_args__ = ({"schema": "testing_schema"})
    ##id = Column(Integer, primary_key=True)
    #bibcode = Column(String(19), primary_key=True)
    #payload = Column(JSON)


def build_raw_citation_table(schema="public"):
    class RawCitation(Base):
        __tablename__ = 'raw_citation'
        __table_args__ = ({"schema": schema})
        id = Column(Integer, primary_key=True)
        bibcode = Column(String(19))
        payload = Column(JSON)
        #payload = Column(JSONB) # Binary, faster searches (requires postgres 9.4)
    return RawCitation

RawCitation = build_raw_citation_table()

