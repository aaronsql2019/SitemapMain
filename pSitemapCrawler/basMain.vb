Imports System
Imports System.Data
Imports System.IO
Imports System.Net
Imports System.Net.Http
Imports System.Text
Imports System.Xml
Imports Microsoft.Data.SqlClient
Imports Microsoft.VisualBasic.FileIO

Module Program
    ' Change Sub Main to Async Function Main and return Task
    Async Function Main() As Task
        Try
            ' Configuration
            Dim sitemapUrl As String = "https://example.com/sitemap.xml" ' Replace with target sitemap URL
            Dim csvFilePath As String = "sitemap_output.csv"
            Dim connectionString As String = "Server=localhost\SQL2022;Database=SitemapScanner;Trusted_Connection=True;"
            Dim csvDelimiter As String = ","

            ' Await the async method
            Await ParseSitemapToCsvAsync(sitemapUrl, csvFilePath, csvDelimiter)

            ' Bulk insert CSV to SQL Server
            BulkInsertToSqlServer(csvFilePath, connectionString)

            Console.WriteLine("Sitemap scanning and data insertion completed successfully.")
        Catch ex As Exception
            Console.WriteLine("Error: " & ex.Message)
        End Try
    End Function

    Async Function ParseSitemapToCsvAsync(sitemapUrl As String, csvFilePath As String, delimiter As String) As Task
        ' Download and parse sitemap XML using HttpClient
        Dim xmlContent As String
        Using client As New HttpClient()
            xmlContent = Await client.GetStringAsync(sitemapUrl)
        End Using

        Dim xmlDoc As New XmlDocument()
        xmlDoc.LoadXml(xmlContent)

        ' Prepare CSV output
        Using writer As New StreamWriter(csvFilePath, False, Encoding.UTF8)
            ' Write CSV header
            writer.WriteLine($"Url{delimiter}LastModified{delimiter}ChangeFrequency{delimiter}Priority{delimiter}ImageUrl{delimiter}ImageCaption{delimiter}ImageTitle")

            ' XML namespaces
            Dim nsManager As New XmlNamespaceManager(xmlDoc.NameTable)
            nsManager.AddNamespace("sm", "http://www.sitemaps.org/schemas/sitemap/0.9")
            nsManager.AddNamespace("image", "http://www.google.com/schemas/sitemap-image/1.1")

            ' Get all URL nodes
            Dim urlNodes As XmlNodeList = xmlDoc.SelectNodes("//sm:url", nsManager)
            For Each urlNode As XmlNode In urlNodes
                Dim url As String = urlNode.SelectSingleNode("sm:loc", nsManager)?.InnerText?.Replace("""", """""")
                Dim lastMod As String = urlNode.SelectSingleNode("sm:lastmod", nsManager)?.InnerText
                Dim changeFreq As String = urlNode.SelectSingleNode("sm:changefreq", nsManager)?.InnerText
                Dim priority As String = urlNode.SelectSingleNode("sm:priority", nsManager)?.InnerText

                ' Handle image nodes
                Dim imageNodes As XmlNodeList = urlNode.SelectNodes("image:image", nsManager)
                If imageNodes.Count = 0 Then
                    ' No images, write URL data only
                    writer.WriteLine($"""{url}""{delimiter}{lastMod}{delimiter}{changeFreq}{delimiter}{priority}{delimiter}{delimiter}")
                Else
                    ' Write a row for each image
                    For Each imageNode As XmlNode In imageNodes
                        Dim imageUrl As String = imageNode.SelectSingleNode("image:loc", nsManager)?.InnerText?.Replace("""", """""")
                        Dim imageCaption As String = imageNode.SelectSingleNode("image:caption", nsManager)?.InnerText?.Replace("""", """""")
                        Dim imageTitle As String = imageNode.SelectSingleNode("image:title", nsManager)?.InnerText?.Replace("""", """""")
                        writer.WriteLine($"""{url}""{delimiter}{lastMod}{delimiter}{changeFreq}{delimiter}{priority}{delimiter}""{imageUrl}""{delimiter}""{imageCaption}""{delimiter}""{imageTitle}""")
                    Next
                End If
            Next
        End Using
    End Function

    Sub BulkInsertToSqlServer(csvFilePath As String, connectionString As String)
        Using conn As New SqlConnection(connectionString)
            conn.Open()
            Using bulkCopy As New SqlBulkCopy(conn)
                bulkCopy.DestinationTableName = "SitemapUrls"
                ' Map CSV columns to SQL table columns
                bulkCopy.ColumnMappings.Add("Url", "Url")
                bulkCopy.ColumnMappings.Add("LastModified", "LastModified")
                bulkCopy.ColumnMappings.Add("ChangeFrequency", "ChangeFrequency")
                bulkCopy.ColumnMappings.Add("Priority", "Priority")
                bulkCopy.ColumnMappings.Add("ImageUrl", "ImageUrl")
                bulkCopy.ColumnMappings.Add("ImageCaption", "ImageCaption")
                bulkCopy.ColumnMappings.Add("ImageTitle", "ImageTitle")

                ' Read CSV and perform bulk insert
                Using reader As New StreamReader(csvFilePath)
                    ' Skip header
                    reader.ReadLine()
                    Using csv As New TextFieldParser(reader)
                        csv.TextFieldType = FieldType.Delimited
                        csv.SetDelimiters(",")
                        csv.HasFieldsEnclosedInQuotes = True

                        While Not csv.EndOfData
                            Dim fields = csv.ReadFields()
                            Using dt As New DataTable()
                                dt.Columns.Add("Url", GetType(String))
                                dt.Columns.Add("LastModified", GetType(DateTime))
                                dt.Columns.Add("ChangeFrequency", GetType(String))
                                dt.Columns.Add("Priority", GetType(Decimal))
                                dt.Columns.Add("ImageUrl", GetType(String))
                                dt.Columns.Add("ImageCaption", GetType(String))
                                dt.Columns.Add("ImageTitle", GetType(String))

                                Dim row = dt.NewRow()
                                row("Url") = fields(0)
                                row("LastModified") = If(String.IsNullOrEmpty(fields(1)), DBNull.Value, DateTime.Parse(fields(1)))
                                row("ChangeFrequency") = If(String.IsNullOrEmpty(fields(2)), DBNull.Value, fields(2))
                                row("Priority") = If(String.IsNullOrEmpty(fields(3)), DBNull.Value, Decimal.Parse(fields(3)))
                                row("ImageUrl") = If(String.IsNullOrEmpty(fields(4)), DBNull.Value, fields(4))
                                row("ImageCaption") = If(String.IsNullOrEmpty(fields(5)), DBNull.Value, fields(5))
                                row("ImageTitle") = If(String.IsNullOrEmpty(fields(6)), DBNull.Value, fields(6))
                                dt.Rows.Add(row)

                                bulkCopy.WriteToServer(dt)
                            End Using
                        End While
                    End Using
                End Using
            End Using
        End Using
    End Sub
End Module