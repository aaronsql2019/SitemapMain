Imports System.Data.SqlClient
Imports System.IO

Module Module1

    Sub Main(args As String())
        Console.WriteLine("IPLookup Database Console Application")
        Console.WriteLine("------------------------------------")

        If args.Length <> 3 Then
            Console.WriteLine("Usage: IPLookupApp.exe <ConnectionString> <SQLStatement> <SourceDNSServerForLog>")
            Console.WriteLine(vbCrLf & "Example ConnectionString (ensure it allows connection to master for DB creation check):")
            Console.WriteLine("""Server=localhost\SQLEXPRESS;Integrated Security=True;TrustServerCertificate=True;""")
            Console.WriteLine(vbCrLf & "Example SQLStatement:")
            Console.WriteLine("""INSERT INTO DomainLookups (DomainURL, IPAddress, SourceDNSServer) VALUES ('example.com', '93.184.216.34', 'dns.example.com')""")
            Console.WriteLine(vbCrLf & "Example SourceDNSServerForLog:")
            Console.WriteLine("""PrimaryInternalDNS""")
            Return
        End If

        Dim userConnectionString As String = args(0)
        Dim sqlUserStatement As String = args(1)
        Dim sourceDnsServerForLog As String = args(2)

        Try
            ' Step 1: Ensure the database and its schema exist
            Dim actualIpLookupsConnectionString As String = EnsureDatabaseAndSchema(userConnectionString)
            Console.WriteLine("Database and schema verified/created successfully.")


            ' Step 2: Execute the provided SQL Statement against IPLookups database
            Using connection As New SqlConnection(actualIpLookupsConnectionString)
                connection.Open()
                Console.WriteLine(vbCrLf & $"Executing SQL Statement: {sqlUserStatement}")
                Using command As New SqlCommand(sqlUserStatement, connection)
                    Dim recordsAffected As Integer = command.ExecuteNonQuery()
                    Console.WriteLine($"{recordsAffected} record(s) affected by the SQL statement.")
                End Using
            End Using
            Console.WriteLine("SQL statement executed successfully.")

            ' Step 3: Call the stored procedure to log the operation
            Using connection As New SqlConnection(actualIpLookupsConnectionString)
                connection.Open()
                Console.WriteLine(vbCrLf & "Calling stored procedure LogDnsOperation...")
                Using command As New SqlCommand("LogDnsOperation", connection)
                    command.CommandType = System.Data.CommandType.StoredProcedure
                    command.Parameters.AddWithValue("@OperationDetails", sqlUserStatement)
                    command.Parameters.AddWithValue("@ServerUsed", sourceDnsServerForLog)
                    command.ExecuteNonQuery()
                End Using
            End Using
            Console.WriteLine("Stored procedure called successfully.")

        Catch ex As SqlException
            Console.ForegroundColor = ConsoleColor.Red
            Console.WriteLine(vbCrLf & $"SQL Error: {ex.Message}")
            For Each err As SqlError In ex.Errors
                Console.WriteLine($"    Error Number: {err.Number}, LineNumber: {err.LineNumber}, Message: {err.Message}")
            Next
            Console.ResetColor()
        Catch ex As Exception
            Console.ForegroundColor = ConsoleColor.Red
            Console.WriteLine(vbCrLf & $"General Error: {ex.Message}")
            Console.WriteLine(ex.ToString())
            Console.ResetColor()
        End Try

        Console.WriteLine(vbCrLf & "Application finished. Press any key to exit.")
        Console.ReadKey()
    End Sub

    ''' <summary>
    ''' Ensures the 'IPLookups' database and its required schema (tables, SP) exist.
    ''' Creates them if they don't.
    ''' </summary>
    ''' <param name="baseConnectionString">User-provided connection string, potentially to a specific DB or just the server.</param>
    ''' <returns>The connection string specifically for the 'IPLookups' database.</returns>
    Function EnsureDatabaseAndSchema(ByVal baseConnectionString As String) As String
        Dim masterConnectionStringBuilder As New SqlConnectionStringBuilder(baseConnectionString)
        Dim targetDbName As String = "IPLookups"

        ' Temporarily set Initial Catalog to master for DB existence check and creation
        Dim originalInitialCatalog As String = masterConnectionStringBuilder.InitialCatalog
        masterConnectionStringBuilder.InitialCatalog = "master"
        ' Add TrustServerCertificate=True if not present, common for local dev SQL Express without proper certs
        If Not masterConnectionStringBuilder.ConnectionString.ToLower().Contains("trustservercertificate") Then
            masterConnectionStringBuilder.TrustServerCertificate = True
        End If
        Dim tempMasterConnString As String = masterConnectionStringBuilder.ConnectionString

        ' --- Check/Create Database ---
        Using masterConnection As New SqlConnection(tempMasterConnString)
            masterConnection.Open()
            Dim checkDbCmdText As String = $"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{targetDbName}') CREATE DATABASE {targetDbName};"
            Console.WriteLine($"Checking/Creating database '{targetDbName}'...")
            Using command As New SqlCommand(checkDbCmdText, masterConnection)
                command.ExecuteNonQuery()
            End Using
            Console.WriteLine($"Database '{targetDbName}' is ready.")
        End Using

        ' --- Prepare connection string for IPLookups database ---
        Dim ipLookupsConnectionStringBuilder As New SqlConnectionStringBuilder(baseConnectionString)
        ipLookupsConnectionStringBuilder.InitialCatalog = targetDbName ' Ensure it points to IPLookups
        ' Add TrustServerCertificate=True if not present
        If Not ipLookupsConnectionStringBuilder.ConnectionString.ToLower().Contains("trustservercertificate") Then
            ipLookupsConnectionStringBuilder.TrustServerCertificate = True
        End If
        Dim actualIpLookupsConnectionString As String = ipLookupsConnectionStringBuilder.ConnectionString

    Private Sub ExecuteNonQuery(ByVal connection As SqlConnection, ByVal commandText As String, ByVal message As String)
        Console.WriteLine(message)
        Using command As New SqlCommand(commandText, connection)
            command.ExecuteNonQuery()
        End Using
    End Sub

End Module