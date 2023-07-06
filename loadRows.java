import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.Buffer;
import java.sql.*;

public class loadRows {

    static final String DB_URL = "jdbc:oracle:thin:@aloe.cs.arizona.edu:1521:oracle";
    static final String USER = "niklauswetter";
    static final String PASS = "a0513";

    public static void main(String[] args){
        String fileName = "2020_torn.csv";
        int batchSize = 20;
        Connection connection = null;

        try{
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO niklauswetter.tornadoes2020 " +
                    "(tornadoNo, tYear, tMonth, tDay, tDate, tTime, timezone, tState, stateFipsNo, stateNo, magnitude, injuries, fatalities, propertyLoss, cropLoss, startingLatitude, startingLongitude, endingLatitude, endingLongitude, tLength, tWidth, statesAffected, completeTrack, segmentNo, county1FipsNo, county2FipsNo, county3FipsNo, county4FipsNo, fScale) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(fileName));
            String lineText = null;

            int count = 0;

            lineReader.readLine(); //skip header

            while((lineText = lineReader.readLine()) != null){
                String[] data = lineText.split(",");
                if(recordIsClean(data)){
                    //Record is clean and can be added

                    //Add TornadoNo
                    statement.setInt(1, Integer.parseInt(data[0]));
                    //Add tYear, tMonth, tDay
                    for(int i = 1; i < 4; i++){
                        if(data[i].equals("NULL"))
                            statement.setNull((i+1), Types.NULL);
                        else
                            statement.setInt((i+1), Integer.parseInt(data[i]));
                    }
                    //Add tDate, tTime
                    for(int i = 4; i < 6; i++){
                        if(data[i].equals("NULL"))
                            statement.setNull((i+1), Types.NULL);
                        else
                            statement.setString((i+1), data[i]);
                    }
                    //Add timezone
                    if(data[6].equals("NULL"))
                        statement.setNull(7, Types.NULL);
                    else
                        statement.setInt(7, Integer.parseInt(data[6]));
                    //Add tState
                    if(data[7].equals("NULL"))
                        statement.setNull(8, Types.NULL);
                    else
                        statement.setString(8, data[7]);
                    //Add stateFipsNo, stateNo, magnitude, injuries, fatalities, propertyLoss, cropLoss
                    for(int i = 8; i < 15; i++){
                        if(data[i].equals("NULL"))
                            statement.setNull((i+1), Types.NULL);
                        else
                            statement.setInt((i+1), Integer.parseInt(data[i]));
                    }
                    //Add startingLatitude, startingLongitude, endingLatitude, endingLongitude, tLength
                    for(int i = 15; i < 20; i++){
                        if(data[i].equals("NULL"))
                            statement.setNull((i+1), Types.NULL);
                        else
                            statement.setDouble((i+1), Double.parseDouble(data[i]));
                    }
                    //Add tWidth, statesAffected, completeTrack, segementNo
                    //county1FipsNo, county2FipsNo, county3FipsNo, county4FipsNo, fScale
                    for(int i = 20; i < 29; i++){
                        if(data[i].equals("NULL"))
                            statement.setNull((i+1), Types.NULL);
                        else
                            statement.setInt((i+1), Integer.parseInt(data[i]));
                    }
                    //Add batch
                    statement.addBatch();
                }

                if(count % batchSize == 0){
                    statement.executeBatch();
                }
            }

            lineReader.close();
            statement.executeBatch();

            connection.commit();
            connection.close();
        }catch(IOException e){
            e.printStackTrace();
        }catch(SQLException e){
            e.printStackTrace();
            try{
                connection.rollback();
            }catch(SQLException ex){
                e.printStackTrace();
            }
        }
    }

    public static boolean recordIsClean(String[] record){
        //tornadoNo
        if(record[0] == null || !record[0].matches("^[0-9]+$"))
            return false;

        for(int i = 1; i < record.length; i++){
            if(record[i]==null || record[i]=="")
                record[i] = "NULL";
        }

        //tYear
        if(record[1].length() != 4 || !record[1].matches("^[0-9]+$"))
            record[1] = "NULL";

        //tMonth
        if(record[2].length() != 2 || !record[2].matches("^[0-9]+$"))
            record[2] = "NULL";

        //tDay
        if(record[3].length() != 2 || !record[3].matches("^[0-9]+$"))
            record[3] = "NULL";

        //tDate
        //regex || !record[4].matches("^\\d{4}\\-(0?[1-9]|1[012])\\-(0?[1-9]|[12][0-9]|3[01])$")
        if(record[4].length() != 10)
            record[4] = "NULL";

        //tTime
        //regex ^(?:[01]\\d|2[0123]):(?:[012345]\\d):(?:[012345]\\d)$
        if(record[5].length() != 8)
            record[5] = "NULL";

        //timezone
        if(record[6].length() > 1 || record[6].equals("?"))
            record[6] = "NULL";

        //tState
        if(record[7].length() != 2)
            record[7] = "NULL";

        //stateFipsNo
        if(!record[8].matches("^[0-9]+$"))
            record[8] = "NULL";

        //stateNo
        //Defunct data, just check if num
        if(!record[9].matches("^[0-9]+$"))
            record[9] = "NULL";

        //magnitude
        if(!record[10].matches("^[0-9]+$"))
            record[10] = "NULL";

        //injuries
        if(!record[11].matches("^[0-9]+$"))
            record[11] = "NULL";

        //fatalities
        if(!record[12].matches("^[0-9]+$"))
            record[12] = "NULL";

        //propertyLoss
        if(!record[13].matches("^[0-9]+$"))
            record[13] = "NULL";

        //cropLoss
        if(!record[14].matches("^[0-9]+$"))
            record[14] = "NULL";

        //startingLatitude
        if(!record[15].matches("^[+-]?(([0-9]\\d*))(\\.\\d+)?"))
            record[15] ="NULL";

        //startingLongitude
        if(!record[16].matches("^[+-]?(([0-9]\\d*))(\\.\\d+)?"))
            record[16] ="NULL";

        //endingLatitude
        if(!record[17].matches("^[+-]?(([0-9]\\d*))(\\.\\d+)?"))
            record[17] ="NULL";

        //endingLongitude
        if(!record[18].matches("^[+-]?(([0-9]\\d*))(\\.\\d+)?"))
            record[18] ="NULL";

        //length
        if(!record[19].matches("^(?:[1-9]\\d*|0)?(?:\\.\\d+)?$"))
            record[19] ="NULL";

        //width
        if(!record[20].matches("^[0-9]+$"))
            record[20] = "NULL";

        //ns
        if(!record[21].equals("1") && !record[21].equals("2") && !record[21].equals("3"))
            record[21] = "NULL";

        //sn
        if(!record[22].equals("0") && !record[22].equals("1"))
            record[22] = "NULL";

        //sg
        if(!record[23].equals("1") && !record[23].equals("2") && !record[23].equals("-9"))
            record[23] = "NULL";

        //fips codes
        for(int i = 24; i < 28; i++){
            if(!record[i].matches("^[0-9]+$"))
                record[i] = "NULL";
        }

        //fScale
        if(!record[28].equals("0") && !record[28].equals("1"))
            record[28] = "NULL";

        return true;
    }
}