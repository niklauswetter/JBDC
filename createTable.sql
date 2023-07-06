CREATE TABLE tornadoes2019(
    tornadoNo NUMBER NOT NULL,
    tYear NUMBER NULL,
    tMonth NUMBER NULL,
    tDay NUMBER NULL,
    tDate VARCHAR2(10) NULL,
    tTime VARCHAR2(8) NULL,
    timezone NUMBER NULL,
    tState VARCHAR2(2) NULL,
    stateFipsNo NUMBER NULL,
    stateNo NUMBER NULL,
    magnitude NUMBER NULL,
    injuries NUMBER NULL,
    fatalities NUMBER NULL,
    propertyLoss NUMBER NULL,
    cropLoss NUMBER NULL,
    startingLatitude NUMBER NULL,
    startingLongitude NUMBER NULL,
    endingLatitude NUMBER NULL,
    endingLongitude NUMBER NULL,
    tLength NUMBER NULL,
    tWidth NUMBER NULL,
    statesAffected NUMBER NULL,
    completeTrack NUMBER NULL,
    segmentNo NUMBER NULL,
    county1FipsNo NUMBER NULL,
    county2FipsNo NUMBER NULL,
    county3FipsNo NUMBER NULL,
    county4FipsNo NUMBER NULL,
    fScale NUMBER NULL
);
