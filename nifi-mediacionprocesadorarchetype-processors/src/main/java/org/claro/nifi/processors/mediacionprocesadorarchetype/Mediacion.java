package org.claro.nifi.processors.mediacionprocesadorarchetype;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Mediacion {
    public String julian() {
        /*
        --> juliano (string) 19352 "año 2019 dia 18/12"
        */
        DateFormat jd = new SimpleDateFormat( "yyDDD" );
        String julian = jd.format( Calendar.getInstance().getTime() );
        return (julian);
    }
    public long timeStampMillis() {
        /*
        --> fecha (long) 1576525718869
        */
        Date date= new Date();
        long time = date.getTime();
        return (time);
    }
 /*   public long stringToEpochTime(String dateTime) throws ParseException {
        /*
        <-- fecha (string) 20191218163520
        --> fecha (long) 1576525718869

        SimpleDateFormat crunchifyFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date formatDate = crunchifyFormat.parse( dateTime );
        long epochTime = formatDate.getTime();
        return (epochTime);
    }*/
 /*   public boolean excludeOld(String schedulerDate, String eventDate, String expirationTime) throws ParseException {
        /*
        <-- fecha fin scheduler (string) 20200105000000
        <-- fecha de evento (string) 20191219094420
        <-- dias de expiracion (string) 91
        --> true/false (expirado o no)

        long expTime = Long.parseLong(expirationTime);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        Date epochSchDate = df.parse(schedulerDate);
        Date epochEvnDate = df.parse(eventDate);
        return (epochSchDate.getTime() - (expTime * 86400000)) > epochEvnDate.getTime();
    }*/
}


