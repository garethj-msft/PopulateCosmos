using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic.FileIO;
// ReSharper disable StyleCop.SA1600

namespace PopulateCosmos
{
    using System.Data;
    using System.Dynamic;
    using System.IO;

    using Gremlin.Net.Driver;
    using Gremlin.Net.Structure.IO.GraphSON;

    using Newtonsoft.Json.Converters;
    using Newtonsoft.Json.Linq;

    public partial class Program
    {
        public static async Task Main(string[] args)
        {
            // Azure Cosmos DB Configuration variables
            string hostname = "https://storageprototype.gremlin.cosmosdb.azure.com";

            int port = 443;

            var authKey = AuthKey;

            string database = "educationDatabase";

            string collection = "educationRoster";

            string directory = null;

            // Assume first arg is the directory containing the CSVs or PWD.
            if (args.Length > 0 && !string.IsNullOrWhiteSpace(args[0]))
            {
                directory = args[0];
            }

            if (directory == null)
            {
                directory = Directory.GetCurrentDirectory();
            }

            var schoolDictionary = CreateJsonDictionaryFromCsv(
                Path.Combine(directory, "school.csv"),
                new Dictionary<string, int>
                    {
                        { "sisId", 0 },
                        { "schoolNumber", 1 },
                        { "name", 4 },
                        { "gradeLow", 5 },
                        { "gradeHigh", 6 },
                        { "principalName", 7 },
                        { "principalSecondaryEmail", 8 },
                        { "address", 9 },
                        { "city", 10 },
                        { "state", 11 },
                        { "zip", 12 },
                        { "phone", 13 },
                        { "principalSisId", 14 }
                    },
                "sisId");

            var sectionDictionary = CreateJsonDictionaryFromCsv(
                Path.Combine(directory, "section.csv"),
                new Dictionary<string, int>
                    {
                        { "sisId", 0 },
                        { "schoolSisId", 1 },
                        { "classNumber", 2 },
                        { "name", 3 },
                        { "termSisId", 4 },
                        { "termName", 5 },
                        { "termStateDate", 6 },
                        { "termEndDate", 7 },
                        { "courseSisId", 8 },
                        { "courseName", 9 },
                        { "courseDescription", 10 },
                        { "courseNumber", 11 },
                        { "courseSubject", 12 },
                        { "periods", 13 }
                    },
                "sisId");

            var userDictionary = CreateJsonDictionaryFromCsv(
                Path.Combine(directory, "student.csv"),
                new Dictionary<string, int>
                    {
                        { "sisId", 0 },
                        { "schoolSisId", 1 },
                        { "stateId", 2 },
                        { "studentNumber", 3 },
                        { "adAlias", 4 },
                        { "firstName", 5 },
                        { "middlename", 6 },
                        { "lastName", 7 },
                        { "mailingAddress", 8 },
                        { "mailingCity", 9 },
                        { "mailingState", 10 },
                        { "mailingCountry", 11 },
                        { "mailingZip", 12 },
                        { "mailingLatitude", 13 },
                        { "mailingLongitude", 14 },
                        { "residenceAddress", 15 },
                        { "residenceCity", 16 },
                        { "residenceState", 17 },
                        { "residenceCountry", 18 },
                        { "residenceZip", 19 },
                        { "residenceLatitude", 20 },
                        { "residenceLongitude", 21 },
                        { "gender", 22 },
                        { "birthDate", 23 },
                        { "grade", 24 },
                        { "ellStatus", 25 },
                        { "federalRace", 26 },
                        { "secondaryEmail", 27 },
                        { "graduationYear", 28 },
                        { "status", 29 },
                        { "username", 30 },
                        { "password", 31 }
                    },
                "sisId");

            AppendToJsonDictionaryFromCsv(
                userDictionary,
                Path.Combine(directory, "teacher.csv"),
                new Dictionary<string, int>
                    {
                        { "sisId", 0 },
                        { "schoolSisId", 1 },
                        { "stateId", 2 },
                        { "teacherNumber", 3 },
                        { "adAlias", 4 },
                        { "status", 5 },
                        { "firstName", 6 },
                        { "middlename", 7 },
                        { "lastName", 8 },
                        { "secondaryEmail", 9 },
                        { "title", 10 },
                        { "qualification", 11 },
                        { "username", 12 },
                        { "password", 13 }
                    },
                "sisId");

            DecorateJsonDictionaryEntriesFromCsv(
                sectionDictionary,
                Path.Combine(directory, "teacherroster.csv"),
                new Dictionary<string, int>
                    {
                        { "sectionSisId", 0 },
                        { "sisId", 1 },
                        { "schoolSisId", 2 },
                        { "personType", 3 },
                        { "status", 4 },
                        { "entryDate", 5 },
                        { "exitDate", 6 },
                    },
                "sectionSisId",
                "teachers");

            DecorateJsonDictionaryEntriesFromCsv(
                sectionDictionary,
                Path.Combine(directory, "studentenrollment.csv"),
                new Dictionary<string, int>
                    {
                        { "sectionSisId", 0 },
                        { "sisId", 1 },
                        { "schoolSisId", 2 },
                        { "personType", 3 },
                        { "status", 4 },
                        { "entryDate", 5 },
                        { "exitDate", 6 },
                    },
                "sectionSisId",
                "students");

            var gremlinServer = new GremlinServer(
                hostname,
                port,
                enableSsl: true,
                username: "/dbs/" + database + "/colls/" + collection,
                password: authKey);

            using (var gremlinClient = new GremlinClient(
                gremlinServer,
                new GraphSON2Reader(),
                new GraphSON2Writer(),
                GremlinClient.GraphSON2MimeType))
            {
                // Clean the database
                var result = await gremlinClient.SubmitAsync<dynamic>("g.V().drop()");

                foreach (var school in schoolDictionary.Values)
                {
                    var command = new StringBuilder("g.addV('school')");
                    foreach (var prop in school.Properties())
                    {
                        command.Append($".property('${prop.Name}', '${prop.Value}')");
                    }

                    result = await gremlinClient.SubmitAsync<dynamic>(command.ToString());
                }
            }

            Console.ReadLine();

            foreach (var section in sectionDictionary.Values)
            {
                Console.WriteLine(section.ToString());
            }

            Console.ReadLine();

            foreach (var user in userDictionary.Values)
            {
                Console.WriteLine(user.ToString());
            }

            Console.ReadLine();
        }

        private static void DecorateJsonDictionaryEntriesFromCsv(Dictionary<string, JObject> dictionaryToDecorate, string csvFile, Dictionary<string, int> fieldMapping, string parentKey, string decorationName)
        {
            var csvStrings = GetCsvContents(csvFile);
            var parentLookup = csvStrings.Select(
                stringArray => CreateJObjectFromStrings(fieldMapping, stringArray)).ToLookup(j => j[parentKey].ToString());
            foreach (var newGroup in parentLookup)
            {
                if (dictionaryToDecorate.TryGetValue(newGroup.Key, out var elementToDecorate))
                {
                    elementToDecorate.Add(decorationName, new JArray(newGroup));
                }
            }
        }

        private static Dictionary<string, JObject> CreateJsonDictionaryFromCsv(
            string csvFile,
            Dictionary<string, int> fieldMapping,
            string key)
        {
            var csvStrings = GetCsvContents(csvFile);
            var dictionary = csvStrings.Select(
                stringArray => CreateJObjectFromStrings(fieldMapping, stringArray)).ToDictionary(j => j[key].ToString());
            return dictionary;
        }

        private static void AppendToJsonDictionaryFromCsv(
            Dictionary<string, JObject> dictionary,
            string csvFile,
            Dictionary<string, int> fieldMapping,
            string key)
        {
            var csvStrings = GetCsvContents(csvFile);
            var newDictionary = csvStrings.Select(
                stringArray => CreateJObjectFromStrings(fieldMapping, stringArray)).ToDictionary(j => j[key].ToString());
            foreach (var newKey in newDictionary.Keys)
            {
                dictionary.Add(newKey, newDictionary[newKey]);
            }
        }


        private static JObject CreateJObjectFromStrings(Dictionary<string, int> fieldMapping, string[] stringArray)
        {
            var j = new JObject();
            foreach (string fieldMappingKey in fieldMapping.Keys)
            {
                j.Add(fieldMappingKey, new JValue(stringArray[fieldMapping[fieldMappingKey]]));
            }

            return j;
        }

        private static IEnumerable<string[]> GetCsvContents(string csvFile)
        {
            using (var file = new FileStream(csvFile, FileMode.Open))
            {
                var result = ParseFile(file, ",", true);
                return result;
            }
        }

        private static IEnumerable<string[]> ParseFile(Stream fileStream, string delimiter, bool skipHeaderLine)
        {
            IList<string[]> result = new List<string[]>();
            TextFieldParser textFieldParser = null;
            try
            {
                textFieldParser = new TextFieldParser(fileStream);
                textFieldParser.TextFieldType = FieldType.Delimited;
                textFieldParser.SetDelimiters(delimiter);
                textFieldParser.HasFieldsEnclosedInQuotes = true;
                textFieldParser.TrimWhiteSpace = true;

                while (!textFieldParser.EndOfData)
                {
                    string[] parsedEntry = textFieldParser.ReadFields();
                    if (skipHeaderLine)
                    {
                        skipHeaderLine = false;
                        continue;
                    }

                    result.Add(parsedEntry);
                }
            }
            finally
            {
                textFieldParser?.Dispose();
            }

            return result;
        }
    }
}
