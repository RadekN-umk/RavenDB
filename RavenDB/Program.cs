using Raven.Client.Documents;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace RavenDB
{
    class Program
    {
        static void RavenDB(string[] args, List<string> NewCreatedFiles, List<string> LastModifiedFiles, List<string> DeletedFiles)
        {
            using (IDocumentStore store = new DocumentStore
            {
                Urls = new[]                        // URL to the Server,
                {                                   // or list of URLs 
                    args[1]                             // to all Cluster Servers (Nodes)
                },
                Database = args[2],                 // Default database that DocumentStore will interact with
                Conventions = { }                   // DocumentStore customizations
            })
            {
                store.Initialize();                 // Each DocumentStore needs to be initialized before use.
                                                    // This process establishes the connection with the Server
                                                    // and downloads various configurations
                                                    // e.g. cluster topology or client configuration

                using (var session = store.OpenSession())
                {
                    foreach (string s in NewCreatedFiles)
                    {
                        using (var file1 = File.Open(s, FileMode.Open))
                        {
                            session.Advanced.Attachments.Store("test", s, file1, "application/json");
                            session.SaveChanges();
                        }
                    }
                    foreach (string s in LastModifiedFiles)
                    {
                        using (var file1 = File.Open(s, FileMode.Open))
                        {
                            session.Advanced.Attachments.Store("test", s, file1, "application/json");
                            session.SaveChanges();

                        }

                    }
                    foreach (string s in DeletedFiles)
                    {
                        session.Advanced.Attachments.Delete("test", s);
                        session.SaveChanges();
                    }
                }
            }
        }

        static void Main(string[] args)
        {
            if (args is null)
            {
                throw new ArgumentNullException(nameof(args) + " cannot be null.");
            }

            if (args.Length != 3)
            {
                throw new ArgumentException($"Number of arguments is incorrect. Expected: 3, given {args.Length}.");
            }

            if (!Directory.Exists(args[0]))
            {
                throw new DirectoryNotFoundException(args[0]);
            }

            
            var directory = new DirectoryInfo(args[0]);
            List<string> DeletedFiles = new List<string>();
            List<string> OldFileList = new List<string>();
            List<string> NewCreatedFiles = directory.GetFiles().Where(f => f.CreationTime >= DateTime.Now - TimeSpan.FromMinutes(5)).Select(f => f.FullName).ToList();
            List<string> LastModifiedFiles = directory.GetFiles().Where(f => f.LastWriteTime >= DateTime.Now - TimeSpan.FromMinutes(5)).Select(f => f.FullName).ToList();

            //History.dat contains history of synchronized json files, if nonexist all files are trated as new and will be uploaded to database 
            if (File.Exists("./History.dat"))
            {
                OldFileList = File.ReadAllLines("./History.dat").ToList();
                DeletedFiles = OldFileList.Except(directory.GetFiles().Select(x => x.FullName)).ToList();
                LastModifiedFiles = LastModifiedFiles.Except(NewCreatedFiles).ToList();
            }
            else
            {
                OldFileList = directory.GetFiles().Select(x => x.FullName).ToList();
                NewCreatedFiles = OldFileList;
                LastModifiedFiles.Clear();
                File.WriteAllLines("./History.dat", OldFileList);
            }

            RavenDB(args, NewCreatedFiles, LastModifiedFiles, DeletedFiles);
           
            File.WriteAllLines("./History.dat", directory.GetFiles().Select(f => f.FullName).ToArray());
            Environment.Exit(0);
        }
    }
}
