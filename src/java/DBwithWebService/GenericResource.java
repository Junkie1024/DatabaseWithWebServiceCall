package DBwithWebService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PUT;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 * REST Web Service
 *
 * @author Jayu
 */
@Path("hrDB")
public class GenericResource {

    static Connection conn = null;
    static Statement stm = null;
    static ResultSet rs = null;

    static JSONObject mainObj = new JSONObject();

    static String jobId, jobTitle, minSalary, maxSalary;

    @Context
    private UriInfo context;

    public GenericResource() {
    }

    @GET
    @Produces("application/xml")
    public String getXml() {

        throw new UnsupportedOperationException();
    }

    public static Connection getConnection() {

        try {

            Class.forName("oracle.jdbc.OracleDriver");
            conn = DriverManager.getConnection("jdbc:oracle:thin:@144.217.163.57:1521:XE", "hr", "inf5180");

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(GenericResource.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(GenericResource.class.getName()).log(Level.SEVERE, null, ex);
        }

        return conn;
    }

    public static void closeConnection() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                /* ignored */
            }
        }
        if (stm != null) {
            try {
                stm.close();
            } catch (SQLException e) {
                /* ignored */
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                /* ignored */
            }
        }
    }

    @GET
    @Path("db")
    @Produces(MediaType.TEXT_PLAIN)
    public String db() {

        mainObj.clear();
        JSONArray mnArray = new JSONArray();
        JSONObject singleobj = new JSONObject();

        conn = getConnection();

        if (conn != null) {
            try {
                String sql = "SELECT JOB_ID,JOB_TITLE,MIN_SALARY,MAX_SALARY FROM JOBS";
                stm = conn.createStatement();
                int i = stm.executeUpdate(sql);

                rs = stm.executeQuery(sql);

                if (rs.next() == true) {
                    mainObj.accumulate("Status", "OK");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
                    do {

                        jobId = rs.getString("JOB_ID");
                        jobTitle = rs.getString("JOB_TITLE");
                        minSalary = rs.getString("MIN_SALARY");
                        maxSalary = rs.getString("MAX_SALARY");
                        singleobj.accumulate("Job_ID", jobId);
                        singleobj.accumulate("Job_Title", jobTitle);
                        singleobj.accumulate("Min_Salary", minSalary);
                        singleobj.accumulate("Max_Salary", maxSalary);
                        mnArray.add(singleobj);
                        singleobj.clear();

                    } while (rs.next());
                    mainObj.accumulate("Jobs", mnArray);
                } else {

                    mainObj.accumulate("Status", "Error");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
                    mainObj.accumulate("Message", "No Records Found");

                }
            } catch (SQLException ex) {
                Logger.getLogger(GenericResource.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeConnection();
            }
        } else {
            mainObj.accumulate("Status", "Error");
            mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
            mainObj.accumulate("Message", "Connection Error");
        }
        return mainObj.toString();
    }

    @GET
    @Path("singlejob&{jobId}")
    @Produces(MediaType.TEXT_PLAIN)
    public String singleJob(@PathParam("jobId") String jobid) {
        mainObj.clear();
        conn = getConnection();

        String sql = "SELECT * FROM JOBS WHERE JOB_ID = '" + jobid + "'";

        if (conn != null) {
            try {
                stm = conn.createStatement();
                int i = stm.executeUpdate(sql);

                rs = stm.executeQuery(sql);

                if (rs.next() == true) {
                    mainObj.accumulate("Status", "OK");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
                    do {

                        jobId = rs.getString("JOB_ID");
                        jobTitle = rs.getString("JOB_TITLE");
                        minSalary = rs.getString("MIN_SALARY");
                        maxSalary = rs.getString("MAX_SALARY");
                        mainObj.accumulate("JobId", jobId);
                        mainObj.accumulate("Job Title", jobTitle);
                        mainObj.accumulate("Minimum Salary", minSalary);
                        mainObj.accumulate("Maximum Salary", maxSalary);
                    } while (rs.next());
                } else {

                    mainObj.accumulate("Status", "Error");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
                    mainObj.accumulate("Message", "Record Not Found");
                }

            } catch (SQLException ex) {
                Logger.getLogger(GenericResource.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeConnection();
            }
        } else {
            mainObj.accumulate("Status", "Error");
            mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
            mainObj.accumulate("Message", "Connection Error");

        }
        return mainObj.toString();
    }

    @GET
    @Path("insert&{jobId}&{jobTitle}&{minSalary}&{maxSalary}")
    @Produces(MediaType.TEXT_PLAIN)
    public String insert(@PathParam("jobId") String jobid, @PathParam("jobTitle") String jobtitle, @PathParam("minSalary") int minsalary, @PathParam("maxSalary") int maxsalary) {

        mainObj.clear();
        conn = getConnection();

        String sql = "INSERT INTO JOBS(JOB_ID,JOB_TITLE,MIN_SALARY,MAX_SALARY) VALUES('" + jobid + "','" + jobtitle + "','" + minsalary + "','" + maxsalary + "')";

        if (conn != null) {
            try {
                stm = conn.createStatement();
                int i = stm.executeUpdate(sql);

                if (i > 0) {
                    mainObj.accumulate("Status", "OK");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
                    mainObj.accumulate("Message", "Record inserted");

                } else {
                    mainObj.accumulate("Status", "Error");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
                    mainObj.accumulate("Message", "Record Not inserted");
                }

            } catch (SQLException ex) {
                Logger.getLogger(GenericResource.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeConnection();
            }
        } else {
            mainObj.accumulate("Status", "Error");
            mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
            mainObj.accumulate("Message", "Connection Error");
        }
        return mainObj.toString();
    }

    
    
    @GET
    @Path("update&{jobId}&{jobTitle}")
    @Produces(MediaType.TEXT_PLAIN)
    public String update(@PathParam("jobId")String jobid, @PathParam("jobtitle")String jobtitle){
        mainObj.clear();
        
        conn = getConnection();
        
        String sql = "UPDATE JOBS SET JOB_TITLE='"+ jobtitle +"' WHERE JOB_ID='"+ jobid +"'";
        
        if (conn != null) {
            try {
                stm = conn.createStatement();

                int i = stm.executeUpdate(sql);

                if (i > 0) {
                    mainObj.accumulate("Status", "OK");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis()/1000);
                    mainObj.accumulate("Message", "Record Updated");
                } else {
                    mainObj.accumulate("Status", "Error");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis()/1000);
                    mainObj.accumulate("Message", "Record Not Updated");
                }

            } catch (SQLException ex) {
                Logger.getLogger(GenericResource.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeConnection();
            }
        } else {
            mainObj.accumulate("Status", "Error");
            mainObj.accumulate("TimeStamp", System.currentTimeMillis()/1000);
            mainObj.accumulate("Message", "Connection Error");
        }
        return mainObj.toString();
    }
    
    @GET
    @Path("delete&{jobId}")
    @Produces(MediaType.TEXT_PLAIN)
    public String delete(@PathParam("jobId") String jobid) {

        mainObj.clear();

        conn = getConnection();

        String sql = "DELETE FROM JOBS WHERE JOB_ID = '" + jobid + "'";

        if (conn != null) {
            try {
                stm = conn.createStatement();
                int i = stm.executeUpdate(sql);
                if (i > 0) {
                    mainObj.accumulate("Status", "OK");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
                    mainObj.accumulate("Message", "Record Deleted");
                } else {
                    mainObj.accumulate("Status", "Error");
                    mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
                    mainObj.accumulate("Message", "Record not Deleted");
                }

            } catch (SQLException ex) {
                Logger.getLogger(GenericResource.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                closeConnection();
            }
        } else {
            mainObj.accumulate("Status", "Error");
            mainObj.accumulate("TimeStamp", System.currentTimeMillis() / 1000);
            mainObj.accumulate("Message", "Connection Error");
        }

        return mainObj.toString();
    }
}
