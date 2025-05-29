Imports System
Imports System.Data.SqlClient
Imports System.Net.Http
Imports System.Text.RegularExpressions
Imports System.Threading.Tasks
Imports HtmlAgilityPack

Module Program
    Private ReadOnly ConnectionString As String = "Server=localhost\SQL2022;Database=SitemapFinder;Trusted_Connection=True;"
    Private ReadOnly HttpClient As New HttpClient()
    Private ReadOnly GoogleSearchUrl As String = "https://www.google.com/search?q=site:{0}+filetype:xml+sitemap"

    Sub Main()
        ' Configure HttpClient
        HttpClient.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        ' Process domains
        ProcessDomains().GetAwaiter().GetResult()
    End Sub

    Async Function ProcessDomains() As Task
        Try
            Using conn As New SqlConnection(ConnectionString)
                Await conn.OpenAsync()

                ' Get domains
                Dim domains As New List(Of (DomainID As Integer, DomainName As String))
                Using cmd As New SqlCommand("SELECT DomainID, DomainName FROM Domains WHERE Status IS NULL OR Status = 'Pending'", conn)
                    Using reader As SqlDataReader = Await cmd.ExecuteReaderAsync()
                        While Await reader.ReadAsync()
                            domains.Add((reader.GetInt32(0), reader.GetString(1)))
                        End While
                    End Using
                End Using

                ' Get default sitemap URLs
                Dim defaultUrls As New Dictionary(Of Integer, (URLPath As String, BitPosition As Integer))
                Using cmd As New SqlCommand("SELECT URLID, URLPath, BitPosition FROM DefaultSitemapURLs", conn)
                    Using reader As SqlDataReader = Await cmd.ExecuteReaderAsync()
                        While Await reader.ReadAsync()
                            defaultUrls.Add(reader.GetInt32(0), (reader.GetString(1), reader.GetInt32(2)))
                        End While
                    End Using
                End Using

                ' Process each domain
                For Each domain In domains
                    Console.WriteLine($"Processing domain: {domain.DomainName}")
                    Await ProcessDomain(conn, domain.DomainID, domain.DomainName, defaultUrls)
                Next
            End Using
        Catch ex As Exception
            Console.WriteLine($"Error: {ex.Message}")
        End Try
    End Function

    Async Function ProcessDomain(conn As SqlConnection, domainID As Integer, domainName As String, defaultUrls As Dictionary(Of Integer, (URLPath As String, BitPosition As Integer))) As Task
        Dim sitemapFound As Boolean = False
        Dim sitemapUrl As String = Nothing
        Dim attemptedUrls As Long = 0

        ' Step 1: Try robots.txt
        sitemapUrl = Await GetSitemapFromRobotsTxt(domainName)
        If Not String.IsNullOrEmpty(sitemapUrl) Then
            Await SaveSitemap(conn, domainID, sitemapUrl, "RobotsTxt")
            sitemapFound = True
        End If

        ' Step 2: Try default sitemap URLs if none found in robots.txt
        If Not sitemapFound Then
            For Each defaultUrl In defaultUrls
                If Await CheckSitemapUrl($"https://{domainName}{defaultUrl.Value.URLPath}") Then
                    Await SaveSitemap(conn, domainID, $"https://{domainName}{defaultUrl.Value.URLPath}", "DefaultURL")
                    sitemapFound = True
                    attemptedUrls = attemptedUrls Or (1L << defaultUrl.Value.BitPosition)
                    Exit For
                Else
                    attemptedUrls = attemptedUrls Or (1L << defaultUrl.Value.BitPosition)
                End If
            Next
        End If

        ' Step 3: Search Google for XML sitemaps if none found
        If Not sitemapFound Then
            Dim googleSitemap As String = Await SearchGoogleForSitemap(domainName)
            If Not String.IsNullOrEmpty(googleSitemap) Then
                Await SaveSitemap(conn, domainID, googleSitemap, "Google")
                sitemapFound = True
            End If
        End If

        ' Update domain status
        Dim status As String = If(sitemapFound, "Processed", "Failed")
        Using cmd As New SqlCommand("UPDATE Domains SET Status = @Status, LastChecked = @LastChecked, SitemapURL = @SitemapURL, AttemptedURLs = @AttemptedURLs WHERE DomainID = @DomainID", conn)
            cmd.Parameters.AddWithValue("@Status", status)
            cmd.Parameters.AddWithValue("@LastChecked", DateTime.Now)
            cmd.Parameters.AddWithValue("@SitemapURL", If(sitemapUrl, DBNull.Value))
            cmd.Parameters.AddWithValue("@AttemptedURLs", attemptedUrls)
            cmd.Parameters.AddWithValue("@DomainID", domainID)
            Await cmd.ExecuteNonQueryAsync()
        End Using
    End Function

    Async Function GetSitemapFromRobotsTxt(domainName As String) As Task(Of String)
        Try
            Dim robotsUrl As String = $"https://{domainName}/robots.txt"
            Dim response As String = Await HttpClient.GetStringAsync(robotsUrl)
            Dim regex As New Regex("Sitemap:\s*(.+)", RegexOptions.IgnoreCase)
            Dim match As Match = regex.Match(response)
            If match.Success Then
                Return match.Groups(1).Value.Trim()
            End If
        Catch
            ' Ignore errors (e.g., 404, timeout)
        End Try
        Return Nothing
    End Function

    Async Function CheckSitemapUrl(url As String) As Task(Of Boolean)
        Try
            Dim response As HttpResponseMessage = Await HttpClient.GetAsync(url)
            If response.IsSuccessStatusCode Then
                Dim content As String = Await response.Content.ReadAsStringAsync()
                Return content.Contains("<sitemap") OrElse content.Contains("<urlset")
            End If
        Catch
            ' Ignore errors
        End Try
        Return False
    End Function

    Async Function SearchGoogleForSitemap(domainName As String) As Task(Of String)
        Try
            Dim searchUrl As String = String.Format(GoogleSearchUrl, domainName)
            Dim response As String = Await HttpClient.GetStringAsync(searchUrl)
            Dim doc As New HtmlDocument()
            doc.LoadHtml(response)

            ' Look for links in search results
            Dim links = doc.DocumentNode.SelectNodes("//a[@href]")
            If links IsNot Nothing Then
                For Each link In links
                    Dim href As String = link.GetAttributeValue("href", "")
                    If href.Contains(".xml") AndAlso href.Contains("sitemap") Then
                        ' Extract URL from Google's redirect
                        Dim regex As New Regex("url\?q=(.+?)&")
                        Dim match As Match = regex.Match(href)
                        If match.Success Then
                            Dim sitemapUrl As String = Uri.UnescapeDataString(match.Groups(1).Value)
                            If Await CheckSitemapUrl(sitemapUrl) Then
                                Return sitemapUrl
                            End If
                        End If
                    End If
                Next
            End If
        Catch
            ' Ignore errors
        End Try
        Return Nothing
    End Function

    Async Function SaveSitemap(conn As SqlConnection, domainID As Integer, sitemapUrl As String, source As String) As Task
        Using cmd As New SqlCommand("INSERT INTO FoundSitemaps (DomainID, SitemapURL, Source) VALUES (@DomainID, @SitemapURL, @Source)", conn)
            cmd.Parameters.AddWithValue("@DomainID", domainID)
            cmd.Parameters.AddWithValue("@SitemapURL", sitemapUrl)
            cmd.Parameters.AddWithValue("@Source", source)
            Await cmd.ExecuteNonQueryAsync()
        End Using
    End Function
End Module
