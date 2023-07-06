import java.sql.*;
import java.util.*;

/* ------------------------------------------------------------------------
 * Program 3 - JDBC
 * ------------------------------------------------------------------------
 * Prog21.java: This program will offer a simple interface with a database containing information on tornadoes from the
 *              years 2017-2020. This functionality will be provided by SQL used within this Java program.
 *
 *              The program will provide the user a menu offering different queries that can be performed on the
 *              database, and then pass this via SQL to the databse. Additional data processing in Java may be used upon
 *              successful retrieval of records.
 *
 *              There will be some queries provided by default that return the following records:
 *                  - Given a year, all states that had 1+ tornadoes that year, listed in descending order
 *                  - All states with non decreasing tornado count and tornado count of 1+, list alphabetically by state
 *                  - All states with non increasing tornado count and tornado count of 1+, list alphabetically by state
 *                      - 1,2,3,4 is non decreasing
 *                      - 4,3,2,1 is non increasing
 *                      - 1,1,1,1 is both
 *                  - Given a state, receive property, crop, and total damage from 2017-2020
 * ------------------------------------------------------------------------
 *     Author: Niklaus Wetter
 *     Course: CSC 460 - Database Design
 * Instructor: Dr. McCann
 *        TAs: Priya  Kaushik
 *             Aayush Pinto
 *   Due Date: November 2, 2022
 * ------------------------------------------------------------------------
 * Java Version: java version "16.0.2" 2021-07-20
 *               Java(TM) SE Runtime Environment (build 16.0.2+7-67)
 *               Java HotSpot(TM) 64-Bit Server VM (build 16.0.2+7-67, mixed mode, sharing)
 * ------------------------------------------------------------------------
 * Special Compilation Requirements: Run this command before compilation
 *                                   export CLASSPATH=/usr/lib/oracle/19.8/client64/lib/ojdbc8.jar:${CLASSPATH}
 * ------------------------------------------------------------------------
 * Missing Features: None
 *             Bugs: None
 * ------------------------------------------------------------------------ */
public class Prog3 {

    /*
     * I felt this was an appropriate case to use global variables since they are static final values that will be used
     * across multiple methods in a single file program as credentials.
     */
    static final String DB_URL = "jdbc:oracle:thin:@aloe.cs.arizona.edu:1521:oracle";
    static final String USER = "niklauswetter";
    static final String PASS = "a0513";

    public static void main(String[] args){
        Scanner scanner = new Scanner(System.in);
        int selection = 0;
        do{
            System.out.println("Select a query option below:");
            System.out.println("[1] Enter a year to receive a list of states that had tornadoes.");
            System.out.println("[2] Show states with non-decreasing and non-increasing quantities of tornadoes.");
            System.out.println("[3] Enter a US state code to receive the combined property and crop damage across 2017-2020.");
            System.out.println("[-1] Exit");
            selection = scanner.nextInt();
            if(selection==1){
                int year = 0;
                System.out.println("Enter a year between 2017-2020:");
                year = scanner.nextInt();
                if(year>=2017 && year<=2020){
                    queryOne(year);
                }else
                    System.out.println("This query requires a year between 2017 and 2020.");
            }else if(selection==2){
                queryTwo();
            }else if(selection==3){
                String stateCode = "";
                System.out.println("Enter a US state code:");
                stateCode = scanner.next().toUpperCase().strip();
                if(stateCode.length() == 2){
                    queryThree(stateCode);
                }else
                    System.out.println("This query requires a two character US state code.");
            }
            System.out.println();
        }while(selection!=-1);
        System.out.println("Program closed");
    }

    /*
     * Method queryOne(year)
     *
     *        Purpose: This method performs all of the logic for query one from our menu. This method will open a
     *                 connection to the DBMS and execute an SQL query, then format the returned data to be displayed to
     *                 the user. The desired data is filtered using both our SQL query and Java logic on the returned
     *                 data. This query will take a year and return the total number of tornadoes for each state during
     *                 that year. States with zero tornadoes will not be displayed; results will be displayed in
     *                 descending order by total number of tornadoes.
     *
     *  Pre-Condition: DBMS must be properly populated and the username and password fields must be valid
     *
     * Post-Condition: The information stated in the purpose is printed to the console
     *
     *     Parameters:
     *         year -- The year which to retrieve the mentioned data from
     *
     *     Returns: void
     */
    public static void queryOne(int year){
        Connection connection = null;
        ArrayList<Object[]> recordSet = new ArrayList<Object[]>();
        try{
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
            connection.setAutoCommit(false);
            String sql = "SELECT tornadoNo, tState, statesAffected, completeTrack, segmentNo FROM niklauswetter.tornadoes"+String.valueOf(year);
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();

            while(resultSet.next()){
                Object[] record = new Object[5];

                Integer tornadoNo = resultSet.getInt("tornadoNo");
                String tState = resultSet.getString("tState");
                Integer statesAffected = resultSet.getInt("statesAffected");
                Integer completeTrack = resultSet.getInt("completeTrack");
                Integer segmentNo = resultSet.getInt("segmentNo");

                record[0] = tornadoNo;
                record[1] = tState;
                record[2] = statesAffected;
                record[3] = completeTrack;
                record[4] = segmentNo;

                recordSet.add(record);
            }
            connection.close();
        }catch (SQLException e){
            e.printStackTrace();
        }

        HashMap<String, Integer> results = new HashMap<String, Integer>();
        HashMap<String, ArrayList<Integer>> idHistory = new HashMap<String, ArrayList<Integer>>();
        for(String s:getStateCodes()){
            results.put(s, 0);
            idHistory.put(s, new ArrayList<Integer>());
        }

        for(Object[] objectArray:recordSet){
            Integer tno = (Integer) objectArray[0];
            String st = (String) objectArray[1];

            if(!idHistory.get(st).contains(tno)){
                //This record's ID is not in the history of this state
                idHistory.get(st).add(tno);
                results.put(st, results.get(st).intValue()+1);
            }
        }

        System.out.println("STATE \t TORNADOCOUNT");
        System.out.println("---------------------");
        for(int i = 0; i < getStateCodes().size(); i++){
            int maxValue = 0;
            String maxState = null;
            for(String s:results.keySet()){
                if(results.get(s).intValue() > maxValue){
                    maxValue = results.get(s).intValue();
                    maxState = s;
                }
            }
            if(maxState!=null)
                System.out.println(maxState+"\t "+maxValue);
            results.remove(maxState);
        }
    }

    /*
     * Method queryTwo()
     *
     *        Purpose: This method performs all of the logic for query two from our menu. This method will open a
     *                 connection to the DBMS and execute an SQL query, then format the returned data to be displayed to
     *                 the user. The desired data is filtered using both our SQL query and Java logic on the returned
     *                 data. This query takes no input parameters and returns a list of non-decreasing and
     *                 non-increasing states in terms of number of tornadoes across the years 2017-2020. The lists will
     *                 be displayed in alphabetical order with only states reporting at least one tornado across those
     *                 four years being displayed.
     *
     *  Pre-Condition: DBMS must be properly populated and the username and password fields must be valid
     *
     * Post-Condition: The information stated in the purpose is printed to the console
     *
     *     Parameters:
     *         none
     *
     *     Returns: void
     */
    public static void queryTwo(){
        Connection connection = null;

        HashMap<String, Integer[]> yearlyTornadoes = new HashMap<String, Integer[]>();
        ArrayList<Object[]> recordSet2017 = new ArrayList<Object[]>();
        ArrayList<Object[]> recordSet2018 = new ArrayList<Object[]>();
        ArrayList<Object[]> recordSet2019 = new ArrayList<Object[]>();
        ArrayList<Object[]> recordSet2020 = new ArrayList<Object[]>();
        try{
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
            connection.setAutoCommit(false);
            String sql1 = "SELECT tornadoNo, tState FROM niklauswetter.tornadoes2017";
            String sql2 = "SELECT tornadoNo, tState FROM niklauswetter.tornadoes2018";
            String sql3 = "SELECT tornadoNo, tState FROM niklauswetter.tornadoes2019";
            String sql4 = "SELECT tornadoNo, tState FROM niklauswetter.tornadoes2020";

            PreparedStatement statement = connection.prepareStatement(sql1);
            ResultSet resultSet2017 = statement.executeQuery();

            statement = connection.prepareStatement(sql2);
            ResultSet resultSet2018 = statement.executeQuery();

            statement = connection.prepareStatement(sql3);
            ResultSet resultSet2019 = statement.executeQuery();

            statement = connection.prepareStatement(sql4);
            ResultSet resultSet2020 = statement.executeQuery();

            for(String s:getStateCodes())
                yearlyTornadoes.put(s, new Integer[]{0,0,0,0});

            while(resultSet2017.next()){
                Object[] record = new Object[2];

                Integer tornadoNo = resultSet2017.getInt("tornadoNo");
                String tState = resultSet2017.getString("tState");

                record[0] = tornadoNo;
                record[1] = tState;

                recordSet2017.add(record);
            }

            while(resultSet2018.next()){
                Object[] record = new Object[2];

                Integer tornadoNo = resultSet2018.getInt("tornadoNo");
                String tState = resultSet2018.getString("tState");

                record[0] = tornadoNo;
                record[1] = tState;

                recordSet2018.add(record);
            }

            while(resultSet2019.next()){
                Object[] record = new Object[2];

                Integer tornadoNo = resultSet2019.getInt("tornadoNo");
                String tState = resultSet2019.getString("tState");

                record[0] = tornadoNo;
                record[1] = tState;

                recordSet2019.add(record);
            }

            while(resultSet2020.next()){
                Object[] record = new Object[2];

                Integer tornadoNo = resultSet2020.getInt("tornadoNo");
                String tState = resultSet2020.getString("tState");

                record[0] = tornadoNo;
                record[1] = tState;

                recordSet2020.add(record);
            }
            connection.close();
        }catch (SQLException e){
            e.printStackTrace();
        }

        HashMap<String, Integer> results2017 = new HashMap<String, Integer>();
        HashMap<String, ArrayList<Integer>> idHistory2017 = new HashMap<String, ArrayList<Integer>>();
        for(String s: getStateCodes()){
            results2017.put(s, 0);
            idHistory2017.put(s, new ArrayList<Integer>());
        }
        for(Object[] objectArray:recordSet2017){
            Integer tno = (Integer) objectArray[0];
            String st = (String) objectArray[1];

            if(!idHistory2017.get(st).contains(tno)){
                idHistory2017.get(st).add(tno);
                results2017.put(st, results2017.get(st).intValue()+1);
            }
        }

        HashMap<String, Integer> results2018 = new HashMap<String, Integer>();
        HashMap<String, ArrayList<Integer>> idHistory2018 = new HashMap<String, ArrayList<Integer>>();
        for(String s: getStateCodes()){
            results2018.put(s, 0);
            idHistory2018.put(s, new ArrayList<Integer>());
        }
        for(Object[] objectArray:recordSet2018){
            Integer tno = (Integer) objectArray[0];
            String st = (String) objectArray[1];

            if(!idHistory2018.get(st).contains(tno)){
                idHistory2018.get(st).add(tno);
                results2018.put(st, results2018.get(st).intValue()+1);
            }
        }

        HashMap<String, Integer> results2019 = new HashMap<String, Integer>();
        HashMap<String, ArrayList<Integer>> idHistory2019 = new HashMap<String, ArrayList<Integer>>();
        for(String s: getStateCodes()){
            results2019.put(s, 0);
            idHistory2019.put(s, new ArrayList<Integer>());
        }
        for(Object[] objectArray:recordSet2019){
            Integer tno = (Integer) objectArray[0];
            String st = (String) objectArray[1];

            if(!idHistory2019.get(st).contains(tno)){
                idHistory2019.get(st).add(tno);
                results2019.put(st, results2019.get(st).intValue()+1);
            }
        }

        HashMap<String, Integer> results2020 = new HashMap<String, Integer>();
        HashMap<String, ArrayList<Integer>> idHistory2020 = new HashMap<String, ArrayList<Integer>>();
        for(String s: getStateCodes()){
            results2020.put(s, 0);
            idHistory2020.put(s, new ArrayList<Integer>());
        }
        for(Object[] objectArray:recordSet2020){
            Integer tno = (Integer) objectArray[0];
            String st = (String) objectArray[1];

            if(!idHistory2020.get(st).contains(tno)){
                idHistory2020.get(st).add(tno);
                results2020.put(st, results2020.get(st).intValue()+1);
            }
        }

        for(String s:getStateCodes()){
            yearlyTornadoes.get(s)[0] = results2017.get(s);
            yearlyTornadoes.get(s)[1] = results2018.get(s);
            yearlyTornadoes.get(s)[2] = results2019.get(s);
            yearlyTornadoes.get(s)[3] = results2020.get(s);
        }

        HashMap<String, Integer[]> nonDecreasing = new HashMap<String, Integer[]>();
        HashMap<String, Integer[]> nonIncreasing = new HashMap<String, Integer[]>();

        for(String s:yearlyTornadoes.keySet()){
            Integer[] temp = yearlyTornadoes.get(s);

            if(temp[0]<=temp[1] && temp[1]<=temp[2] && temp[2]<=temp[3] && (temp[0] > 0 || temp[1] > 0 || temp[2] > 0 || temp[3] > 0)){
                //Non decreasing
                nonDecreasing.put(s, temp);
            }

            if(temp[0]>=temp[1] && temp[1]>=temp[2] && temp[2]>=temp[3] && (temp[0] > 0 || temp[1] > 0 || temp[2] > 0 || temp[3] > 0)){
                //Non increasing
                nonIncreasing.put(s, temp);
            }
        }

        ArrayList<String> alpha = getStateCodes();
        Collections.sort(alpha);

        if(!nonIncreasing.isEmpty()){
            System.out.println("NON INCREASING");
            System.out.println("STATE \t YEARLYTORNADOES");
            for(int i = 0; i < alpha.size(); i++){
                if(nonIncreasing.containsKey(alpha.get(i))){
                    System.out.print(alpha.get(i)+"\t [");
                    for(Integer in:nonIncreasing.get(alpha.get(i))){
                        System.out.print(in.intValue()+" ");
                    }
                    System.out.print("\b]\n");
                }
            }
        }else{
            System.out.println("THERE ARE NO NON INCREASING STATES");
        }

        System.out.println();

        if(!nonDecreasing.isEmpty()){
            System.out.println("\nNON DECREASING");
            System.out.println("STATE \t YEARLYTORNADOES");

            for(int i = 0; i < alpha.size(); i++){
                if(nonDecreasing.containsKey(alpha.get(i))){
                    System.out.print(alpha.get(i)+"\t [");
                    for(Integer in:nonDecreasing.get(alpha.get(i))){
                        System.out.print(in.intValue()+" ");
                    }
                    System.out.print("\b]\n");
                }
            }
        }else{
            System.out.println("THERE ARE NO NON DECREASING STATES");
        }
    }

    /*
     * Method queryThree(stateCode)
     *
     *        Purpose: This method performs all of the logic for query one from our menu. This method will open a
     *                 connection to the DBMS and execute an SQL query, then format the returned data to be displayed to
     *                 the user. The desired data is filtered using both our SQL query and Java logic on the returned
     *                 data. This query will take a two character US state code and return the total amount of property
     *                 and crop damage for that state across the year 2017-2020. The results will be displayed with the
     *                 two categories separated and the total displayed below.
     *
     *  Pre-Condition: DBMS must be properly populated and the username and password fields must be valid
     *
     * Post-Condition: The information stated in the purpose is printed to the console
     *
     *     Parameters:
     *         stateCode -- The two character US state code for which to retrieve data from
     *
     *     Returns: void
     */
    public static void queryThree(String stateCode){
        //Input verification
        ArrayList<String> stateCodeList = getStateCodes();
        if(!stateCodeList.contains(stateCode)){
            System.out.println("This query requires a valid US state code.");
            return;
        }

        Connection connection = null;
        long propertyLossTotal = 0;
        long cropLossTotal = 0;

        try{
            connection = DriverManager.getConnection(DB_URL, USER, PASS);
            connection.setAutoCommit(false);
            String sql1 = "SELECT propertyLoss, cropLoss FROM niklauswetter.tornadoes2017 WHERE tState='"+stateCode+"'";
            String sql2 = "SELECT propertyLoss, cropLoss FROM niklauswetter.tornadoes2018 WHERE tState='"+stateCode+"'";
            String sql3 = "SELECT propertyLoss, cropLoss FROM niklauswetter.tornadoes2019 WHERE tState='"+stateCode+"'";
            String sql4 = "SELECT propertyLoss, cropLoss FROM niklauswetter.tornadoes2020 WHERE tState='"+stateCode+"'";

            PreparedStatement statement = connection.prepareStatement(sql1);
            ResultSet resultSet2017 = statement.executeQuery();

            statement = connection.prepareStatement(sql2);
            ResultSet resultSet2018 = statement.executeQuery();

            statement = connection.prepareStatement(sql3);
            ResultSet resultSet2019 = statement.executeQuery();

            statement = connection.prepareStatement(sql4);
            ResultSet resultSet2020 = statement.executeQuery();

            while(resultSet2017.next()){
                long propertyLoss = resultSet2017.getLong("propertyLoss");
                long cropLoss = resultSet2017.getLong("cropLoss");

                propertyLossTotal+=propertyLoss;
                cropLossTotal+=cropLoss;
            }

            while(resultSet2018.next()){
                long propertyLoss = resultSet2018.getLong("propertyLoss");
                long cropLoss = resultSet2018.getLong("cropLoss");

                propertyLossTotal+=propertyLoss;
                cropLossTotal+=cropLoss;
            }

            while(resultSet2019.next()){
                long propertyLoss = resultSet2019.getLong("propertyLoss");
                long cropLoss = resultSet2019.getLong("cropLoss");

                propertyLossTotal+=propertyLoss;
                cropLossTotal+=cropLoss;
            }

            while(resultSet2020.next()){
                long propertyLoss = resultSet2020.getLong("propertyLoss");
                long cropLoss = resultSet2020.getLong("cropLoss");

                propertyLossTotal+=propertyLoss;
                cropLossTotal+=cropLoss;
            }

            connection.close();
        }catch(SQLException e){
            e.printStackTrace();
        }

        System.out.println("Total Property and Crop Damage in "+stateCode+" from 2017-2020");
        System.out.println("Total Property Damage: "+propertyLossTotal);
        System.out.println("Total Crop Damage: "+cropLossTotal);
        System.out.println("Total Overall Damage: "+(propertyLossTotal+cropLossTotal));
    }

    /*
     * Method getStateCodes()
     *
     *        Purpose: This is a simple helper method that returns an ArrayList object of all the two character US state
     *                 codes. It is used throughout the program mainly for looping through collections and populating
     *                 Map structures with keys.
     *
     *  Pre-Condition: None
     *
     * Post-Condition: Results in a created ArrayList object with a String type and the data listed above
     *
     *     Parameters:
     *         none
     *
     *     Returns: ArrayList<String>
     */
    public static ArrayList<String> getStateCodes(){
        ArrayList<String> result = new ArrayList<String>();
        result.add("AL");
        result.add("AK");
        result.add("AZ");
        result.add("AR");
        result.add("CA");
        result.add("CO");
        result.add("CT");
        result.add("DE");
        result.add("DC");
        result.add("FL");
        result.add("GA");
        result.add("HI");
        result.add("ID");
        result.add("IL");
        result.add("IN");
        result.add("IA");
        result.add("KS");
        result.add("KY");
        result.add("LA");
        result.add("ME");
        result.add("MD");
        result.add("MA");
        result.add("MI");
        result.add("MN");
        result.add("MS");
        result.add("MO");
        result.add("MT");
        result.add("NE");
        result.add("NV");
        result.add("NH");
        result.add("NJ");
        result.add("NM");
        result.add("NY");
        result.add("NC");
        result.add("ND");
        result.add("OH");
        result.add("OK");
        result.add("OR");
        result.add("PA");
        result.add("PR");
        result.add("RI");
        result.add("SC");
        result.add("SD");
        result.add("TN");
        result.add("TX");
        result.add("UT");
        result.add("VT");
        result.add("VA");
        result.add("VI");
        result.add("WA");
        result.add("WV");
        result.add("WI");
        result.add("WY");
        return result;
    }
}