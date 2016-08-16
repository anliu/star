using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CA5
{
    class Program
    {
        static string azBlobContainer = "https://passtorage.blob.core.windows.net/2016cn/";
        static string azBlobContainerRawcn = "https://passtorage.blob.core.windows.net/rawcn/";

        static string azStorageKey = "";

        static StorageCredentials azStorageCred = new StorageCredentials("passtorage", azStorageKey);

        static void Main(string[] args)
        {
            if (args.Length == 1)
            {
                ProcessBlob(args[0]);
            }
            else if (args.Length == 2)
            {
                var startDate = int.Parse(args[0]);
                var endDate = int.Parse(args[1]);
                for (var i = startDate; i <= endDate; i++)
                {
                    ProcessBlob(i.ToString() + "sh");
                    ProcessBlob(i.ToString() + "sz");
                }
            }
        }

        static void ProcessBlob(string blobName)
        {
            var blob = azBlobContainer + blobName + ".7z";
            var tempEnv = Environment.GetEnvironmentVariable("Temp");
            var file = Path.Combine(tempEnv, blobName + ".7z");
            var folder = Path.Combine(tempEnv, blobName);

            DownloadBlob(blob, file);

            ExtractPackage(file, folder);

            UploadContent(folder);
        }

        static void DownloadBlob(string blob, string file)
        {
            var cb = new CloudBlob(new Uri(blob), azStorageCred);
            if (cb.Exists())
            {
                cb.DownloadToFile(file, FileMode.OpenOrCreate);
            }
        }

        static void ExtractPackage(string file, string folder)
        {
            if (!File.Exists(file) || Directory.Exists(folder))
            {
                return;
            }

            var sysDrive = Environment.GetEnvironmentVariable("SystemDrive");
            if (string.IsNullOrEmpty(sysDrive))
            {
                sysDrive = "D:";
            }

            var si = new ProcessStartInfo
            {
                Arguments = "e \"" + file + "\" -o\"" + folder + "\"",
                FileName = Path.Combine(sysDrive, "\\7zip", "7za.exe"),//@"c:\program files\7-zip\7z.exe",
                UseShellExecute = false
            };
            using (var p = Process.Start(si))
            {
                p.WaitForExit();
            }

            File.Delete(file);
        }

        static void UploadContent(string folder)
        {
            if (!Directory.Exists(folder))
            {
                return;
            }

            foreach (var fn in Directory.GetFiles(folder))
            {
                var fileName = Path.GetFileName(fn);
                var uri = new Uri(azBlobContainerRawcn + Path.GetFileName(folder).Substring(8) + "/" + fileName.Substring(8) + "/" + fileName.Substring(0, 8));

                try
                {
                    var cbb = new CloudBlockBlob(uri, azStorageCred);
                    cbb.UploadFromFile(fn);
                }
                finally
                {
                    File.Delete(fn);
                }
            }
        }
    }
}
