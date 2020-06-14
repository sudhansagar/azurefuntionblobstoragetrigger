package com.function.blobtrigger;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import com.sendgrid.*;
import com.sendgrid.helpers.mail.Mail;
import com.sendgrid.helpers.mail.objects.Attachments;
import com.sendgrid.helpers.mail.objects.Content;
import com.sendgrid.helpers.mail.objects.Email;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Base64;

/**
 * Azure Functions with Azure Blob trigger.
 */
public class BlobTriggerJava1 {

    private String apiKey = "xxxxx";
    private String connString = "xxx";

    /**
     * This function will be invoked when a new or updated blob is detected at the specified path. The blob contents are provided as input to this function.
     */
    @FunctionName("BlobTriggerJava1")
    @StorageAccount("AzureWebJobsStorage")
    public void run(
            @BlobTrigger(name = "file", path = "openbankingpoc/{name}", dataType = "binary") byte[] content,
            @BindingName("name") String name,
            final ExecutionContext context
    ) {
        context.getLogger().info("Blob trigger function processed a blob. Name: " + name + "\n  Size: " + content.length + " Bytes");

        String customerEmailId = getCustomerEmail(context, name);
        context.getLogger().info("Sending email to : " + customerEmailId);

        sendEmail(context, name, customerEmailId, content);
    }

    private String getCustomerEmail(ExecutionContext context, String name) {
        context.getLogger().info("Querying for the blob name : " + name);
        String email = null;
        // Connect to database
     /*   String hostName = "your_server.database.windows.net"; // update me
        String dbName = "your_database"; // update me
        String user = "your_username"; // update me
        String password = "your_password"; // update me
        String url = String.format("jdbc:sqlserver://%s:1433;database=%s;user=%s;password=%s;encrypt=true;"
                + "hostNameInCertificate=*.database.windows.net;loginTimeout=30;", hostName, dbName, user, password);*/

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connString);
            String schema = connection.getSchema();
            //System.out.println("Successful connection - Schema: " + schema);

            //System.out.println("Query data example:");
            //System.out.println("=========================================");

            // Create and execute a SELECT SQL statement.
            String selectSql = "SELECT cust.EMAIL FROM [dbo].[CUSTOMER_HOME_LOAN_APPLN] cust WHERE cust.APPLN_NAME=" + "'" + name + "'";
            //context.getLogger().info("Final Query  : " + selectSql);

            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(selectSql)) {

                // Print results from select statement
                while (resultSet.next()) {
                    email = resultSet.getString(1);
                    context.getLogger().info("Query response email : " + email);
                }
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return email;
    }

    private void sendEmail(ExecutionContext context, String name, String customerEmailId, byte[] content) {
        Email from = new Email("sudhansagar@gmail.com");
        String subject = "Loan Application has been received";
        Email to = new Email(customerEmailId);
        Content mailContent = new Content("text/html", "<html><body>Please find the attachment for your reference</body></html>");
        Mail mail = new Mail(from, subject, to, mailContent);

        Attachments attachments = new Attachments();
        attachments.setFilename(name);
        attachments.setType("application/pdf");
        attachments.setDisposition("attachment");

        byte[] attachmentContentBytes = new byte[0];
        try {
            attachmentContentBytes = content;

            String attachmentContent = Base64.getEncoder().encodeToString(attachmentContentBytes);
            attachments.setContent(attachmentContent);
            mail.addAttachments(attachments);

            SendGrid sg = new SendGrid(apiKey);
            Request request = new Request();

            request.setMethod(Method.POST);
            request.setEndpoint("mail/send");
            request.setBody(mail.build());
            Response response = sg.api(request);
            System.out.println(response.getStatusCode());
            System.out.println(response.getBody());
            System.out.println(response.getHeaders());

        } catch (IOException ex) {
            try {
                throw ex;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        context.getLogger().info("Sending mail....");
    }
}
