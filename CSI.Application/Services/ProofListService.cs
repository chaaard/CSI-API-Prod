using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using Microsoft.VisualBasic.FileIO;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.EntityFrameworkCore;
using CSI.Infrastructure.Data;
using System.Security.Cryptography.X509Certificates;
using CSI.Application.DTOs;
using AutoMapper.Configuration.Annotations;
using EFCore.BulkExtensions;
using Newtonsoft.Json;

namespace CSI.Application.Services
{
    public class ProofListService : IProofListService
    {
        private readonly AppDBContext _dbContext;
        private readonly IAnalyticsService _iAnalyticsService;

        public ProofListService(AppDBContext dBContext, IAnalyticsService iAnalyticsService)
        {
            _dbContext = dBContext;
            _dbContext.Database.SetCommandTimeout(999);
            _iAnalyticsService = iAnalyticsService;
        }

        public async Task<(List<Prooflist>?, string?)> ReadProofList(List<IFormFile> files, string customerName, string strClub, string selectedDate, string analyticsParamsDto)
        {
            int row = 2;
            int rowCount = 0;
            var club = Convert.ToInt32(strClub);
            var proofList = new List<Prooflist>();
            var param = new AnalyticsParamsDto();

            if (analyticsParamsDto != null)
            {
                 param = JsonConvert.DeserializeObject<AnalyticsParamsDto>(analyticsParamsDto);
            }

            Dictionary<string, string> customers = new Dictionary<string, string>
            {
                { "GrabFood", "011929" },
                { "GrabMart", "011955" },
                { "PickARooMerch", "011931" },
                { "PickARooFS", "011935" },
                { "FoodPanda", "011838" },
                { "MetroMart", "011855" }
            };

            customers.TryGetValue(customerName, out string valueCust);
            DateTime date;
            if (DateTime.TryParse(selectedDate, out date))
            {
                var GetAnalytics = await _dbContext.Analytics.Where(x => x.CustomerId.Contains(valueCust) && x.LocationId == club && x.TransactionDate == date).AnyAsync();
                if (!GetAnalytics)
                {
                    return (proofList, "No analytics found.");
                }
            }
               
            try
            {
                foreach (var file in files)
                {
                    using (var stream = file.OpenReadStream())
                    {
                        if (file.FileName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
                        {
                            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
                            using (var package = new ExcelPackage(stream))
                            {
                                if (package.Workbook.Worksheets.Count > 0)
                                {
                                    var worksheet = package.Workbook.Worksheets[0]; // Assuming data is in the first worksheet
                                     
                                    rowCount = worksheet.Dimension.Rows;

                                    // Check if the filename contains the word "grabfood"
                                    if (customerName == "GrabMart" || customerName == "GrabFood")
                                    {
                                        var grabProofList = ExtractGrabMartOrFood(worksheet, rowCount, row, customerName, club, selectedDate);
                                        if (grabProofList.Item1 == null)
                                        {
                                            return (null, grabProofList.Item2);
                                        }
                                        else if (grabProofList.Item1.Count <= 0)
                                        {
                                            return (null, grabProofList.Item2);
                                        }
                                        else
                                        {
                                            foreach (var item in grabProofList.Item1)
                                            {
                                                proofList.Add(item);
                                            }
                                        }
                                    }
                                    else if (customerName == "PickARooFS" || customerName == "PickARooMerch")
                                    {
                                        var pickARooProofList = ExtractPickARoo(worksheet, rowCount, row, customerName, club, selectedDate);
                                        if (pickARooProofList.Item1 == null)
                                        {
                                            return (null, pickARooProofList.Item2);
                                        }
                                        else if (pickARooProofList.Item1.Count <= 0)
                                        {
                                            return (null, pickARooProofList.Item2);
                                        }
                                        else
                                        {
                                            foreach (var item in pickARooProofList.Item1)
                                            {
                                                proofList.Add(item);
                                            }
                                        }
                                    }
                                    else if (customerName == "FoodPanda")
                                    {
                                        var foodPandaProofList = ExtractFoodPanda(worksheet, rowCount, row, customerName, club, selectedDate);
                                        if (foodPandaProofList.Item1 == null)
                                        {
                                            return (null, foodPandaProofList.Item2);
                                        }
                                        else if (foodPandaProofList.Item1.Count <= 0)
                                        {
                                            return (null, foodPandaProofList.Item2);
                                        }
                                        else
                                        {
                                            foreach (var item in foodPandaProofList.Item1)
                                            {
                                                proofList.Add(item);
                                            }
                                        }
                                    }
                                    else if (customerName == "MetroMart")
                                    {
                                        var metroMartProofList = ExtractMetroMart(worksheet, rowCount, row, customerName, club, selectedDate);
                                        if (metroMartProofList.Item1 == null)
                                        {
                                            return (null, metroMartProofList.Item2);
                                        }
                                        else if (metroMartProofList.Item1.Count <= 0)
                                        {
                                            return (null, metroMartProofList.Item2);
                                        }
                                        else
                                        {
                                            foreach (var item in metroMartProofList.Item1)
                                            {
                                                proofList.Add(item);
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    return (null, "No worksheets found in the workbook.");
                                }
                            }
                        }
                        else if (file.FileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                        {
                            var tempCsvFilePath = Path.GetTempFileName() + ".csv";

                            using (var fileStream = new FileStream(tempCsvFilePath, FileMode.Create))
                            {
                                file.CopyTo(fileStream);
                            }

                            if (customerName == "GrabMart" || customerName == "GrabFood")
                            {
                                var grabProofList = ExtractCSVGrabMartOrFood(tempCsvFilePath, customerName, club, selectedDate);
                                if (grabProofList.Item1 == null)
                                {
                                    return (null, grabProofList.Item2);
                                }
                                else if (grabProofList.Item1.Count <= 0)
                                {
                                    return (null, grabProofList.Item2);
                                }
                                else
                                {
                                    foreach (var item in grabProofList.Item1)
                                    {
                                        proofList.Add(item);
                                    }
                                }
                            }
                            else if (customerName == "PickARooFS" || customerName == "PickARooMerch")
                            {
                                var pickARooProofList = ExtractCSVPickARoo(tempCsvFilePath, club, selectedDate, customerName);
                                if (pickARooProofList.Item1 == null)
                                {
                                    return (null, pickARooProofList.Item2);
                                }
                                else if (pickARooProofList.Item1.Count <= 0)
                                {
                                    return (null, pickARooProofList.Item2);
                                }
                                else
                                {
                                    foreach (var item in pickARooProofList.Item1)
                                    {
                                        proofList.Add(item);
                                    }
                                }
                            }
                            else if (customerName == "FoodPanda")
                            {
                                var foodPandaProofList = ExtractCSVFoodPanda(tempCsvFilePath, club, selectedDate);
                                if (foodPandaProofList.Item1 == null)
                                {
                                    return (null, foodPandaProofList.Item2);
                                }
                                else if (foodPandaProofList.Item1.Count <= 0)
                                {
                                    return (null, foodPandaProofList.Item2);
                                }
                                else
                                {
                                    foreach (var item in foodPandaProofList.Item1)
                                    {
                                        proofList.Add(item);
                                    }
                                }
                            }
                            else if (customerName == "MetroMart")
                            {
                                var metroMartProofList = ExtractCSVMetroMart(tempCsvFilePath, club, selectedDate);
                                if (metroMartProofList.Item1 == null)
                                {
                                    return (null, metroMartProofList.Item2);
                                }
                                else if (metroMartProofList.Item1.Count <= 0)
                                {
                                    return (null, metroMartProofList.Item2);
                                }
                                else
                                {
                                    foreach (var item in metroMartProofList.Item1)
                                    {
                                        proofList.Add(item);
                                    }
                                }
                            }
                        }
                        else
                        {
                            return (null, "No worksheets.");
                        }
                    }
                }

                if (DataExistsInDatabase(proofList))
                {
                    customers.TryGetValue(customerName, out string value);
                    var convertDate = GetDateTime(selectedDate);
                    DeleteRecords(club, convertDate, value);
                }

                if (proofList != null)
                {
                    await _dbContext.Prooflist.AddRangeAsync(proofList);
                    await _dbContext.SaveChangesAsync();
                    await _iAnalyticsService.UpdateUploadStatus(param);

                    return (proofList, "Success");
                }
                else
                {
                    return (proofList, "No list found.");
                }
            }
            catch (Exception ex)
            {
                return (null, $"Please check error in row {row}: {ex.Message}");
                throw;
            }
        }

        private void DeleteRecords(int club, DateTime? selectedDate, string customerId)
        {
            var dataToDelete = _dbContext.Prooflist
                .Where(x => x.CustomerId.Contains(customerId) && x.TransactionDate == selectedDate && x.StoreId == club)
                .ToList();

            if (dataToDelete != null)
            {
                _dbContext.Prooflist.RemoveRange(dataToDelete);
                _dbContext.SaveChanges();
            }
        }

        private bool DataExistsInDatabase(List<Prooflist> grabProofList)
        {
            // Check if any item in grabProofList exists in the database
            var anyDataExists = grabProofList.Any(item =>
                _dbContext.Prooflist.Any(x =>
                    x.CustomerId == item.CustomerId &&
                    x.TransactionDate == item.TransactionDate &&
                    x.OrderNo == item.OrderNo
                )
            );

            return anyDataExists;
        }

        public DateTime? GetDateTime(object cellValue)
        {
            if (cellValue != null)
            {
                if (DateTime.TryParse(cellValue.ToString(), out var transactionDate))
                {
                    return transactionDate.Date;
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        private (List<Prooflist>, string?) ExtractGrabMartOrFood(ExcelWorksheet worksheet, int rowCount, int row, string customerName, int club, string selectedDate)
        {
            var getLocation = _dbContext.Locations.ToList();
            var grabFoodProofList = new List<Prooflist>();

            // Define expected headers
            string[] expectedHeaders = { "store name", "updated on", "type", "status", "short order id", "net sales" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();

            try
            {
                // Find column indexes based on header names
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                {
                    var header = worksheet.Cells[1, col].Text.ToLower().Trim();
                    if (!string.IsNullOrEmpty(header))
                    {
                        columnIndexes[header] = col;
                    }
                }

                // Check if all expected headers exist in the first row
                foreach (var expectedHeader in expectedHeaders)
                {
                    if (!columnIndexes.ContainsKey(expectedHeader))
                    {
                        return (grabFoodProofList, $"Column not found.");
                    }
                }

                var merchantName = worksheet.Cells[2, columnIndexes["type"]].Value?.ToString();

                if (merchantName != customerName)
                {
                    return (grabFoodProofList, "Uploaded file merchant do not match.");
                }

                for (row = 2; row <= rowCount; row++)
                {
                    if (worksheet.Cells[row, columnIndexes["net sales"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["short order id"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["status"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["updated on"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["store name"]].Value != null)
                    {
                       
                        var transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["updated on"]].Value);

                        var chktransactionDate = new DateTime();
                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }
                        var cnvrtDate = GetDateTime(selectedDate);
                        if (cnvrtDate == chktransactionDate)
                        {
                            var prooflist = new Prooflist
                            {
                                CustomerId = customerName == "GrabMart" ? "9999011955" : "9999011929",
                                TransactionDate = transactionDate,
                                OrderNo = worksheet.Cells[row, columnIndexes["short order id"]].Value?.ToString(),
                                NonMembershipFee = (decimal?)0.00,
                                PurchasedAmount = (decimal?)0.00,
                                Amount = worksheet.Cells[row, columnIndexes["net sales"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["net sales"]].Value?.ToString()) : null,
                                StatusId = worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Cancelled" ? 4 : 3,
                                StoreId = club,
                                DeleteFlag = false,
                            };
                            grabFoodProofList.Add(prooflist);
                        }
                        else
                        {
                            return (grabFoodProofList, "Uploaded file transaction dates do not match.");
                        }
                    }
                }

                return (grabFoodProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception)
            {
                return (grabFoodProofList, "Error extracting proof list.");
            }
        }

        private (List<Prooflist>, string?) ExtractPickARoo(ExcelWorksheet worksheet, int rowCount, int row, string customerName, int club, string selectedDate)
        {
            var pickARooProofList = new List<Prooflist>();

            // Define expected headers
            string[] expectedHeaders = { "order date", "order number", "order status", "amount" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();

            try
            {
                // Find column indexes based on header names
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                {
                    var header = worksheet.Cells[1, col].Text.ToLower().Trim();
                    if (!string.IsNullOrEmpty(header))
                    {
                        columnIndexes[header] = col;
                    }
                }

                // Check if all expected headers exist in the first row
                foreach (var expectedHeader in expectedHeaders)
                {
                    if (!columnIndexes.ContainsKey(expectedHeader))
                    {
                        return (pickARooProofList, $"Column not found.");
                    }
                }

                for (row = 2; row <= rowCount; row++)
                {
                    if (worksheet.Cells[row, columnIndexes["order date"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["order number"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["order status"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["amount"]].Value != null)
                    {

                        var transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["order date"]].Value);

                        var chktransactionDate = new DateTime();
                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }

                        var convertDate = GetDateTime(selectedDate);
                        if (convertDate == chktransactionDate)
                        {
                            if (transactionDate != null)
                            {
                                if (convertDate == chktransactionDate)
                                {
                                    var prooflist = new Prooflist
                                    {
                                        CustomerId = customerName == "PickARooMerch" ? "9999011931" : "9999011935",
                                        TransactionDate = transactionDate,
                                        OrderNo = worksheet.Cells[row, columnIndexes["order number"]].Value?.ToString(),
                                        NonMembershipFee = (decimal?)0.00,
                                        PurchasedAmount = (decimal?)0.00,
                                        Amount = worksheet.Cells[row, columnIndexes["amount"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["amount"]].Value?.ToString()) : null,
                                        StatusId = worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Cancelled" ? 4 : 3,
                                        StoreId = club,
                                        DeleteFlag = false,
                                    };
                                    pickARooProofList.Add(prooflist);
                                }
                                else
                                {
                                    return (pickARooProofList, "Uploaded file transaction dates do not match.");
                                }
                            }
                        }
                    }
                }

                return (pickARooProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception)
            {
                return (pickARooProofList, "Error extracting proof list.");
            }
        }

        private (List<Prooflist>, string?) ExtractFoodPanda(ExcelWorksheet worksheet, int rowCount, int row, string customerName, int club, string selectedDate)
        {
            var foodPandaProofList = new List<Prooflist>();
            var transactionDate = new DateTime?();

            // Define expected headers
            string[] expectedHeaders = { "order id", "order status", "delivered at", "subtotal", "cancelled at", "is payable" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();

            try
            {
                // Find column indexes based on header names
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                {
                    var header = worksheet.Cells[2, col].Text.ToLower().Trim();
                    if (!string.IsNullOrEmpty(header))
                    {
                        columnIndexes[header] = col;
                    }
                }

                // Check if all expected headers exist in the first row
                foreach (var expectedHeader in expectedHeaders)
                {
                    if (!columnIndexes.ContainsKey(expectedHeader))
                    {
                        return (foodPandaProofList, $"Column not found.");
                    }
                }

                for (row = 3; row <= rowCount; row++)
                {
                    if (worksheet.Cells[row, columnIndexes["order id"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["order status"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["delivered at"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["subtotal"]].Value != null)
                    {

                        if (worksheet.Cells[row, columnIndexes["order status"]].Value.ToString() == "Cancelled" && worksheet.Cells[row, columnIndexes["is payable"]].Value.ToString() == "yes")
                        {
                            transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["cancelled at"]].Value);
                        }
                        else if (worksheet.Cells[row, columnIndexes["order status"]].Value.ToString() == "Delivered")
                        {
                            transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["delivered at"]].Value);
                        }
                        else
                        {
                            transactionDate = null;
                        }

                        var chktransactionDate = new DateTime();
                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }


                        var convertDate = GetDateTime(selectedDate);
                        if (convertDate == chktransactionDate)
                        {
                            if (transactionDate != null)
                            {
                                if (convertDate == chktransactionDate)
                                {
                                    var prooflist = new Prooflist
                                    {
                                        CustomerId = "9999011838",
                                        TransactionDate = transactionDate,
                                        OrderNo = worksheet.Cells[row, columnIndexes["order id"]].Value?.ToString(),
                                        NonMembershipFee = (decimal?)0.00,
                                        PurchasedAmount = (decimal?)0.00,
                                        Amount = worksheet.Cells[row, columnIndexes["subtotal"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["subtotal"]].Value?.ToString()) : null,
                                        StatusId = worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Cancelled" && worksheet.Cells[row, columnIndexes["is payable"]].Value.ToString() == "yes" ? 3 : 3,
                                        StoreId = club,
                                        DeleteFlag = false,
                                    };
                                    foodPandaProofList.Add(prooflist);
                                }
                                else
                                {
                                    return (foodPandaProofList, "Uploaded file transaction dates do not match.");
                                }
                            }
                        }
                    }
                }

                return (foodPandaProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception)
            {
                return (foodPandaProofList, "Error extracting proof list.");
            }
        }

        private (List<Prooflist>, string?) ExtractMetroMart(ExcelWorksheet worksheet, int rowCount, int row, string customerName, int club, string selectedDate)
        {
            var metroMartProofList = new List<Prooflist>();

            // Define expected headers
            string[] expectedHeaders = { "jo #", "jo delivery status", "completed date", "non membership fee", "purchased amount" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();

            try
            {
                // Find column indexes based on header names
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                {
                    var header = worksheet.Cells[1, col].Text.ToLower().Trim();
                    if (!string.IsNullOrEmpty(header))
                    {
                        columnIndexes[header] = col;
                    }
                }

                // Check if all expected headers exist in the first row
                foreach (var expectedHeader in expectedHeaders)
                {
                    if (!columnIndexes.ContainsKey(expectedHeader))
                    {
                        return (metroMartProofList, $"Column not found.");
                    }
                }

                for (row = 2; row <= rowCount; row++)
                {
                    if (worksheet.Cells[row, columnIndexes["jo #"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["jo delivery status"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["completed date"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["non membership fee"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["purchased amount"]].Value != null)
                    {

                        var transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["completed date"]].Value);
                        decimal NonMembershipFee = worksheet.Cells[row, columnIndexes["non membership fee"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["non membership fee"]].Value?.ToString()) : 0;
                        decimal PurchasedAmount = worksheet.Cells[row, columnIndexes["purchased amount"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["purchased amount"]].Value?.ToString()) : 0;
                        var chktransactionDate = new DateTime();
                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }

                        var convertDate = GetDateTime(selectedDate);
                        if (convertDate == chktransactionDate)
                        {
                            if (transactionDate != null)
                            {
                                if (convertDate == chktransactionDate)
                                {
                                    var prooflist = new Prooflist
                                    {
                                        CustomerId = "9999011855",
                                        TransactionDate = transactionDate,
                                        OrderNo = worksheet.Cells[row, columnIndexes["jo #"]].Value?.ToString(),
                                        NonMembershipFee = NonMembershipFee,
                                        PurchasedAmount = PurchasedAmount,
                                        Amount = NonMembershipFee + PurchasedAmount,
                                        StatusId = worksheet.Cells[row, columnIndexes["jo delivery status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["jo delivery status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["jo delivery status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["jo delivery status"]].Value?.ToString() == "Cancelled" ? 4 : 3,
                                        StoreId = club,
                                        DeleteFlag = false,
                                    };
                                    metroMartProofList.Add(prooflist);
                                }
                                else
                                {
                                    return (metroMartProofList, "Uploaded file transaction dates do not match.");
                                }
                            }
                        }
                    }
                }

                return (metroMartProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception)
            {
                return (metroMartProofList, "Error extracting proof list.");
            }
        }
        
        private (List<Prooflist>, string?) ExtractCSVGrabMartOrFood(string filePath, string customerName, int club, string selectedDate)
        {
            int row = 2;
            int rowCount = 0;
            var grabFoodProofLists = new List<Prooflist>();
            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();

            try
            {
                string[] expectedHeaders = { "store name", "updated on", "type", "status", "short order id", "net sales" };
                using (var parser = new TextFieldParser(filePath))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    parser.SetDelimiters(",");

                    if (!parser.EndOfData)
                    {
                        // Read and store the headers
                        string[] headers = parser.ReadFields();

                        // Find column indexes based on header names
                        for (int col = 0; col < headers.Length; col++)
                        {
                            var header = headers[col].ToLower().Trim();
                            if (!string.IsNullOrEmpty(header))
                            {
                                columnIndexes[header] = col;
                            }
                        }

                        // Check if all expected headers exist in the first row
                        foreach (var expectedHeader in expectedHeaders)
                        {
                            if (!columnIndexes.ContainsKey(expectedHeader.ToLower()))
                            {
                                return (grabFoodProofLists, $"Column not found.");
                            }
                        }
                    }

                    while (!parser.EndOfData)
                    {
                        string[] fields = parser.ReadFields();
                        var transactionDate = GetDateTime(fields[columnIndexes["updated on"]]);
                        var chktransactionDate = new DateTime();
                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }
                        var cnvrtDate = GetDateTime(selectedDate);
                        if (cnvrtDate == chktransactionDate)
                        {
                            var grabfood = new Prooflist
                            {
                                CustomerId = customerName == "GrabMart" ? "9999011955" : "9999011929",
                                TransactionDate = fields[columnIndexes["updated on"]].ToString() != "" ? GetDateTime(fields[columnIndexes["updated on"]]) : null,
                                OrderNo = fields[columnIndexes["short order id"]],
                                NonMembershipFee = (decimal?)0.00,
                                PurchasedAmount = (decimal?)0.00,
                                Amount = fields[columnIndexes["net sales"]] != "" ? decimal.Parse(fields[columnIndexes["net sales"]]) : (decimal?)0.00,
                                StatusId = fields[columnIndexes["status"]] == "Completed" || fields[columnIndexes["status"]] == "Delivered" || fields[columnIndexes["status"]] == "Transferred" ? 3 : fields[columnIndexes["status"]] == "Cancelled" ? 4 : 3,
                                StoreId = club,
                                DeleteFlag = false,
                            };

                            grabFoodProofLists.Add(grabfood);
                            rowCount++;
                        }
                        else
                        {
                            return (grabFoodProofLists, "Uploaded file transaction dates do not match.");
                        }
                    }
                }

                return (grabFoodProofLists, rowCount.ToString() + " rows extracted");
            }
            catch (Exception ex)
            {
                return (null, $"Please check error in row {rowCount}: {ex.Message}");
            }
        }

        private (List<Prooflist>, string?) ExtractCSVMetroMart(string filePath, int club, string selectedDate)
        {
            int row = 2;
            int rowCount = 0;
            var metroMartProofLists = new List<Prooflist>();
            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            try
            {
                string[] expectedHeaders = { "jo #", "jo delivery status", "completed date", "non membership fee", "purchased amount" };
                using (var parser = new TextFieldParser(filePath))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    parser.SetDelimiters(",");

                    if (!parser.EndOfData)
                    {
                        // Read and store the headers
                        string[] headers = parser.ReadFields();

                        // Find column indexes based on header names
                        for (int col = 0; col < headers.Length; col++)
                        {
                            var header = headers[col].ToLower().Trim();
                            if (!string.IsNullOrEmpty(header))
                            {
                                columnIndexes[header] = col;
                            }
                        }

                        // Check if all expected headers exist in the first row
                        foreach (var expectedHeader in expectedHeaders)
                        {
                            if (!columnIndexes.ContainsKey(expectedHeader.ToLower()))
                            {
                                return (metroMartProofLists, $"Column not found.");
                            }
                        }
                    }

                    while (!parser.EndOfData)
                    {
                        string[] fields = parser.ReadFields();
                        var chktransactionDate = new DateTime();
                        var transactionDate = GetDateTime(fields[columnIndexes["completed date"]]);
                        var NonMembershipFee = fields[columnIndexes["non membership fee"]] != "" ? decimal.Parse(fields[columnIndexes["non membership fee"]]) : (decimal?)0.00;
                        var PurchasedAmount = fields[columnIndexes["purchased amount"]] != "" ? decimal.Parse(fields[columnIndexes["purchased amount"]]) : (decimal?)0.00;
                        var amount = NonMembershipFee + PurchasedAmount;
                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }
                        var cnvrtDate = GetDateTime(selectedDate);

                        var convertDate = GetDateTime(selectedDate);
                        if (convertDate == chktransactionDate)
                        {
                            if (transactionDate != null)
                            {
                                if (convertDate == chktransactionDate)
                                {
                                    var metroMart = new Prooflist
                                    {
                                        CustomerId = "9999011855",
                                        TransactionDate = transactionDate,
                                        OrderNo = fields[columnIndexes["jo #"]],
                                        NonMembershipFee = NonMembershipFee,
                                        PurchasedAmount = PurchasedAmount,
                                        Amount = amount,
                                        StatusId = fields[columnIndexes["jo delivery status"]] == "Completed" || fields[columnIndexes["jo delivery status"]] == "Delivered" || fields[columnIndexes["jo delivery status"]] == "Transferred" ? 3 : fields[columnIndexes["jo delivery status"]] == "Cancelled" ? 4 : 3,
                                        StoreId = club,
                                        DeleteFlag = false,
                                    };

                                    metroMartProofLists.Add(metroMart);
                                    rowCount++;
                                }
                                else
                                {
                                    return (metroMartProofLists, "Uploaded file transaction dates do not match.");
                                }
                            }
                        }
                    }
                }

                return (metroMartProofLists, rowCount.ToString() + " rows extracted");
            }
            catch (Exception ex)
            {
                return (null, $"Please check error in row {row}: {ex.Message}");
            }
        }

        private (List<Prooflist>, string?) ExtractCSVPickARoo(string filePath, int club, string selectedDate, string customerName)
        {
            int row = 2;
            int rowCount = 0;
            var pickARooProofLists = new List<Prooflist>();
            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();

            try
            {
                string[] expectedHeaders = { "order date", "order number", "order status", "amount" };
                using (var parser = new TextFieldParser(filePath))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    parser.SetDelimiters(",");

                    if (!parser.EndOfData)
                    {
                        // Read and store the headers
                        string[] headers = parser.ReadFields();

                        // Find column indexes based on header names
                        for (int col = 0; col < headers.Length; col++)
                        {
                            var header = headers[col].ToLower().Trim();
                            if (!string.IsNullOrEmpty(header))
                            {
                                columnIndexes[header] = col;
                            }
                        }

                        // Check if all expected headers exist in the first row
                        foreach (var expectedHeader in expectedHeaders)
                        {
                            if (!columnIndexes.ContainsKey(expectedHeader.ToLower()))
                            {
                                return (pickARooProofLists, $"Column not found.");
                            }
                        }
                    }

                    while (!parser.EndOfData)
                    {
                        string[] fields = parser.ReadFields();
                        var transactionDate = GetDateTime(fields[columnIndexes["order date"]]); 
                        var chktransactionDate = new DateTime();
                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }

                        var convertDate = GetDateTime(selectedDate);
                        if (convertDate == chktransactionDate)
                        {
                            if (transactionDate != null)
                            {
                                if (convertDate == chktransactionDate)
                                {
                                    var pickARoo = new Prooflist
                                    {
                                        CustomerId = customerName == "PickARooMerch" ? "9999011931" : "9999011935",
                                        TransactionDate = fields[columnIndexes["order date"]].ToString() != "" ? GetDateTime(fields[columnIndexes["order date"]]) : null,
                                        OrderNo = fields[columnIndexes["order number"]],
                                        NonMembershipFee = (decimal?)0.00,
                                        PurchasedAmount = (decimal?)0.00,
                                        Amount = fields[columnIndexes["amount"]] != "" ? decimal.Parse(fields[columnIndexes["amount"]]) : (decimal?)0.00,
                                        StatusId = fields[columnIndexes["order status"]] == "Completed" || fields[columnIndexes["order status"]] == "Delivered" || fields[columnIndexes["order status"]] == "Transferred" ? 3 : fields[columnIndexes["order status"]] == "Cancelled" ? 4 : 3,
                                        StoreId = club,
                                        DeleteFlag = false,
                                    };

                                    pickARooProofLists.Add(pickARoo);
                                    rowCount++;
                                }
                                else
                                {
                                    return (pickARooProofLists, "Uploaded file transaction dates do not match.");
                                }
                            }
                        }
                    }
                }

                return (pickARooProofLists, rowCount.ToString() + " rows extracted");
            }
            catch (Exception ex)
            {
                return (null, $"Please check error in row {rowCount}: {ex.Message}");
            }
        }

        private (List<Prooflist>, string?) ExtractCSVFoodPanda(string filePath, int club, string selectedDate)
        {
            int row = 2;
            int rowCount = 0;
            var foodPandaProofLists = new List<Prooflist>();
            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            try
            {
                string[] expectedHeaders = { "order id", "order status", "delivered at", "subtotal", "cancelled at", "is payable" };

                using (var parser = new TextFieldParser(filePath))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    parser.SetDelimiters(",");

                    if (!parser.EndOfData)
                    {
                        // Read and store the headers
                        string[] headers = parser.ReadFields();

                        for (int col = 0; col < headers.Length; col++)
                        {
                            var header = headers[col].ToLower().Trim();
                            if (!string.IsNullOrEmpty(header))
                            {
                                columnIndexes[header] = col;
                            }
                        }

                        // Check if all expected headers exist in the first row
                        foreach (var expectedHeader in expectedHeaders)
                        {
                            if (!columnIndexes.ContainsKey(expectedHeader.ToLower()))
                            {
                                return (foodPandaProofLists, $"Column not found.");
                            }
                        }
                    }

                    while (!parser.EndOfData)
                    {
                        string[] fields = parser.ReadFields();
                        var chktransactionDate = new DateTime();
                        var transactionDate = new DateTime?();
                        var status = fields[columnIndexes["order status"]].ToLower();
                        var isPayable = fields[columnIndexes["is payable"]].ToLower();

                        var test = fields[columnIndexes["order id"]];

                        if (status == "cancelled" && isPayable == "yes")
                        {
                            transactionDate = GetDateTime(fields[columnIndexes["cancelled at"]]);
                        }
                        else if (status == "delivered")
                        {
                            transactionDate = GetDateTime(fields[columnIndexes["delivered at"]]);
                        }
                        else
                        {
                            transactionDate = null;
                        }

                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }

                        var convertDate = GetDateTime(selectedDate);
                        if (convertDate == chktransactionDate)
                        {
                            if (transactionDate != null)
                            {
                                if (convertDate == chktransactionDate)
                                {
                                    var foodPanda = new Prooflist
                                    {
                                        CustomerId = "9999011838",
                                        TransactionDate = transactionDate,
                                        OrderNo = fields[columnIndexes["order id"]],
                                        NonMembershipFee = (decimal?)0.00,
                                        PurchasedAmount = (decimal?)0.00,
                                        Amount = decimal.Parse(fields[columnIndexes["subtotal"]]),
                                        StatusId = status == "completed" || status == "delivered" ? 3 : status == "cancelled" && isPayable == "yes" ? 3 : 3,
                                        StoreId = club,
                                        DeleteFlag = false,
                                    };

                                    foodPandaProofLists.Add(foodPanda);
                                    rowCount++;
                                }
                                else
                                {
                                    return (foodPandaProofLists, "Uploaded file transaction dates do not match.");
                                }
                            }
                        }
                    }
                }

                return (foodPandaProofLists, rowCount.ToString() + " rows extracted");
            }
            catch (Exception ex)
            {
                return (null, $"Please check error in row {row}: {ex.Message}");
            }
        }


        public int? GetLocationId(string? location, List<Location> locations)
        {
            if (location != null || location != string.Empty)
            {
                string locationName = location.Replace("S&R", "KAREILA");

                var getLocationCode = locations.Where(x => x.LocationName.ToLower().Contains(locationName.ToLower()))
                    .Select(n => n.LocationCode)
                    .FirstOrDefault();
                return getLocationCode;
            }
            else
            {
                return null;
            }
        }

        public async Task<List<PortalDto>> GetPortal(PortalParamsDto portalParamsDto)
        {
            var date = GetDateTime(portalParamsDto.dates[0].Date);
            var result = await _dbContext.Prooflist
                .Join(_dbContext.Locations, a => a.StoreId, b => b.LocationCode, (a, b) => new { a, b })
                .Join(_dbContext.Status, c => c.a.StatusId, d => d.Id, (c, d) => new { c, d })
                .Where(x => x.c.a.TransactionDate.Value.Date == date
                    && x.c.a.StoreId == portalParamsDto.storeId[0]
                    && x.c.a.CustomerId == portalParamsDto.memCode[0]
                    && x.c.a.StatusId != 4)
                .Select(n => new PortalDto
                {
                    Id = n.c.a.Id,
                    CustomerId = n.c.a.CustomerId,
                    TransactionDate = n.c.a.TransactionDate,
                    OrderNo = n.c.a.OrderNo,
                    NonMembershipFee = n.c.a.NonMembershipFee,
                    PurchasedAmount = n.c.a.PurchasedAmount,
                    Amount = n.c.a.Amount,
                    Status = n.d.StatusName,
                    StoreName = n.c.b.LocationName,
                    DeleteFlag = n.c.a.DeleteFlag
                })
                .ToListAsync();

            return result;
        }

        public async Task<(List<AccountingProoflist>?, string?)> ReadAccountingProofList(List<IFormFile> files, string customerName)
        {
            int row = 2;
            int rowCount = 0;
            var proofList = new List<AccountingProoflist>();

            Dictionary<string, string> customers = new Dictionary<string, string>
            {
                {  "9999011929", "GrabFood" },
                {  "9999011955", "GrabMart" },
                {  "9999011931", "PickARooMerch" },
                {  "9999011935", "PickARooFS" },
                {  "9999011838", "FoodPanda" },
                {  "9999011855", "MetroMart" }
            };

            customers.TryGetValue(customerName, out string valueCust);

            try
            {
                foreach (var file in files)
                {
                    using (var stream = file.OpenReadStream())
                    {
                        if (file.FileName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
                        {
                            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
                            using (var package = new ExcelPackage(stream))
                            {
                                if (package.Workbook.Worksheets.Count > 0)
                                {
                                    var worksheet = package.Workbook.Worksheets[0]; 

                                    rowCount = worksheet.Dimension.Rows;

                                    // Check if the filename contains the word "grabfood"
                                    if (valueCust == "GrabMart" || valueCust == "GrabFood")
                                    {
                                        var grabProofList = await ExtractAccountingGrabMartOrFood(worksheet, rowCount, row, file.FileName.ToString(), customerName);
                                        if (grabProofList.Item1 == null)
                                        {
                                            return (null, grabProofList.Item2);
                                        }
                                        else if (grabProofList.Item1.Count <= 0)
                                        {
                                            return (null, grabProofList.Item2);
                                        }
                                        else
                                        {
                                            foreach (var item in grabProofList.Item1)
                                            {
                                                proofList.Add(item);
                                            }
                                        }
                                    }
                                    else if (valueCust == "PickARooFS" || valueCust == "PickARooMerch")
                                    {
                                        var pickARooProofList = await ExtractAccountingPickARoo(worksheet, rowCount, row, file.FileName.ToString(), customerName);
                                        if (pickARooProofList.Item1 == null)
                                        {
                                            return (null, pickARooProofList.Item2);
                                        }
                                        else if (pickARooProofList.Item1.Count <= 0)
                                        {
                                            return (null, pickARooProofList.Item2);
                                        }
                                        else
                                        {
                                            foreach (var item in pickARooProofList.Item1)
                                            {
                                                proofList.Add(item);
                                            }
                                        }
                                    }
                                    else if (valueCust == "FoodPanda")
                                    {
                                        var foodPandaProofList = await ExtractAccountingFoodPanda(worksheet, rowCount, row, file.FileName.ToString(), customerName);
                                        if (foodPandaProofList.Item1 == null)
                                        {
                                            return (null, foodPandaProofList.Item2);
                                        }
                                        else if (foodPandaProofList.Item1.Count <= 0)
                                        {
                                            return (null, foodPandaProofList.Item2);
                                        }
                                        else
                                        {
                                            foreach (var item in foodPandaProofList.Item1)
                                            {
                                                proofList.Add(item);
                                            }
                                        }
                                    }
                                    else if (valueCust == "MetroMart")
                                    {
                                        var metroMartProofList = await ExtractAccountingMetroMart(worksheet, rowCount, row, file.FileName.ToString(), customerName);
                                        if (metroMartProofList.Item1 == null)
                                        {
                                            return (null, metroMartProofList.Item2);
                                        }
                                        else if (metroMartProofList.Item1.Count <= 0)
                                        {
                                            return (null, metroMartProofList.Item2);
                                        }
                                        else
                                        {
                                            foreach (var item in metroMartProofList.Item1)
                                            {
                                                proofList.Add(item);
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    return (null, "No worksheets found in the workbook.");
                                }
                            }
                        }
                        else
                        {
                            return (null, "No worksheets.");
                        }
                    }
                }

                if (proofList != null)
                {
                    await _dbContext.AccountingProoflists.AddRangeAsync(proofList);
                    await _dbContext.SaveChangesAsync();
                    return (proofList, "Success");
                }
                else
                {
                    return (proofList, "No list found.");
                }
            }
            catch (Exception ex)
            {
                return (null, $"Please check error in row {row}: {ex.Message}");
                throw;
            }
        }

        private async Task<(List<AccountingProoflist>, string?)> ExtractAccountingGrabMartOrFood(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo)
        {
            var grabFoodProofList = new List<AccountingProoflist>();
            var fileId = 0;
            // Define expected headers
            string[] expectedHeaders = { "updated on", "store name", "short order id", "net sales", "channel commission" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();

            Dictionary<string, string> customers = new Dictionary<string, string>
            {
                {  "9999011929", "Grab Food" },
                {  "9999011955", "Grab Mart" },
                {  "9999011931", "Pick A Roo - Merch" },
                {  "9999011935", "Pick A Roo - FS" },
                {  "9999011838", "Food Panda" },
                {  "9999011855", "MetroMart" }
            };

            customers.TryGetValue(customerNo, out string valueCust);

            try
            {
              
                // Find column indexes based on header names
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                {
                    var header = worksheet.Cells[1, col].Text.ToLower().Trim();
                    if (!string.IsNullOrEmpty(header))
                    {
                        columnIndexes[header] = col;
                    }
                }

                // Check if all expected headers exist in the first row
                foreach (var expectedHeader in expectedHeaders)
                {
                    if (!columnIndexes.ContainsKey(expectedHeader))
                    {
                        return (grabFoodProofList, $"Column not found.");
                    }
                }

                DateTime date;
                if (DateTime.TryParse(DateTime.Now.ToString(), out date))
                {
                    var fileDescriptions = new FileDescriptions
                    {
                        FileName = fileName,
                        UploadDate = date,
                        Merchant = valueCust,
                        Count = rowCount - 1,
                    };
                    _dbContext.FileDescription.Add(fileDescriptions);
                    await _dbContext.SaveChangesAsync();

                    fileId = fileDescriptions.Id;
                }

                for (row = 2; row <= rowCount; row++)
                {
                    if (worksheet.Cells[row, columnIndexes["updated on"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["store name"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["short order id"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["channel commission"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["net sales"]].Value != null)
                    {
                        var orderNo = worksheet.Cells[row, columnIndexes["short order id"]].Value?.ToString();
                        decimal agencyfee = worksheet.Cells[row, columnIndexes["channel commission"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["channel commission"]].Value?.ToString()) : 0;
                        if (orderNo.Contains("GF"))
                        {
                            var transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["updated on"]].Value);
                            var storeName = worksheet.Cells[row, columnIndexes["store name"]].Value?.ToString();
                            decimal TotalPurchasedAmount = worksheet.Cells[row, columnIndexes["net sales"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["net sales"]].Value?.ToString()) : 0;
                            var chktransactionDate = new DateTime();
                            if (transactionDate.HasValue)
                            {
                                chktransactionDate = transactionDate.Value.Date;
                            }

                            if (transactionDate != null)
                            {
                                var prooflist = new AccountingProoflist
                                {
                                    CustomerId = customerNo,
                                    TransactionDate = transactionDate,
                                    OrderNo = orderNo,
                                    NonMembershipFee = (decimal?)0.00,
                                    PurchasedAmount = (decimal?)0.00,
                                    Amount = TotalPurchasedAmount,
                                    StatusId = 3,
                                    StoreId = await ReturnLocation(storeName),
                                    FileDescriptionId = fileId,
                                    AgencyFee = agencyfee,
                                    DeleteFlag = false,
                                };
                                grabFoodProofList.Add(prooflist);
                            }
                        }
                        else
                        {
                            var transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["updated on"]].Value);
                            var storeName = worksheet.Cells[row, columnIndexes["store name"]].Value?.ToString();
                            decimal TotalPurchasedAmount = worksheet.Cells[row, columnIndexes["net sales"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["net sales"]].Value?.ToString()) : 0;
                            var chktransactionDate = new DateTime();
                            if (transactionDate.HasValue)
                            {
                                chktransactionDate = transactionDate.Value.Date;
                            }

                            if (transactionDate != null)
                            {
                                var prooflist = new AccountingProoflist
                                {
                                    CustomerId = "9999011955",
                                    TransactionDate = transactionDate,
                                    OrderNo = orderNo,
                                    NonMembershipFee = (decimal?)0.00,
                                    PurchasedAmount = (decimal?)0.00,
                                    Amount = TotalPurchasedAmount,
                                    StatusId = 3,
                                    StoreId = await ReturnLocation(storeName),
                                    FileDescriptionId = fileId,
                                    AgencyFee = agencyfee,
                                    DeleteFlag = false,
                                };
                                grabFoodProofList.Add(prooflist);
                            }
                        }
                    }
                }

                return (grabFoodProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception)
            {
                return (grabFoodProofList, "Error extracting proof list.");
            }
        }

        private async Task<(List<AccountingProoflist>, string?)> ExtractAccountingPickARoo(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo)
        {
            var pickARooProofList = new List<AccountingProoflist>();
            var fileId = 0;
            // Define expected headers
            string[] expectedHeaders = { "order number", "outlet name", "order delivery date", "subtotal", "non membership fee amount" };
            string[] expectedHeadersFS = { "order number", "outlet name", "order delivery date", "total" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            Dictionary<string, string> customers = new Dictionary<string, string>
            {
                {  "9999011929", "Grab Food" },
                {  "9999011955", "Grab Mart" },
                {  "9999011931", "Pick A Roo - Merch" },
                {  "9999011935", "Pick A Roo - FS" },
                {  "9999011838", "Food Panda" },
                {  "9999011855", "MetroMart" }
            };

            customers.TryGetValue(customerNo, out string valueCust);

            try
            {
                if (customerNo == "9999011931")
                {
                    // Find column indexes based on header names
                    for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                    {
                        var header = worksheet.Cells[1, col].Text.ToLower().Trim();
                        if (!string.IsNullOrEmpty(header))
                        {
                            columnIndexes[header] = col;
                        }
                    }

                    // Check if all expected headers exist in the first row
                    foreach (var expectedHeader in expectedHeaders)
                    {
                        if (!columnIndexes.ContainsKey(expectedHeader))
                        {
                            return (pickARooProofList, $"Column not found.");
                        }
                    }

                    DateTime date;
                    if (DateTime.TryParse(DateTime.Now.ToString(), out date))
                    {
                        var fileDescriptions = new FileDescriptions
                        {
                            FileName = fileName,
                            UploadDate = date,
                            Merchant = valueCust,
                            Count = rowCount,
                        };
                        _dbContext.FileDescription.Add(fileDescriptions);
                        await _dbContext.SaveChangesAsync();

                        fileId = fileDescriptions.Id;
                    }

                    for (row = 2; row <= rowCount; row++)
                    {
                        if (worksheet.Cells[row, columnIndexes["order number"]].Value != null ||
                            worksheet.Cells[row, columnIndexes["outlet name"]].Value != null ||
                            worksheet.Cells[row, columnIndexes["order delivery date"]].Value != null ||
                            worksheet.Cells[row, columnIndexes["subtotal"]].Value != null ||
                            worksheet.Cells[row, columnIndexes["non membership fee amount"]].Value != null)
                        {

                            var transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["order delivery date"]].Value);
                            decimal NonMembershipFee = worksheet.Cells[row, columnIndexes["non membership fee amount"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["non membership fee amount"]].Value?.ToString()) : 0;
                            decimal SubTotal = worksheet.Cells[row, columnIndexes["subtotal"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["subtotal"]].Value?.ToString()) : 0;
                            var chktransactionDate = new DateTime();
                            if (transactionDate.HasValue)
                            {
                                chktransactionDate = transactionDate.Value.Date;
                            }

                            if (transactionDate != null)
                            {
                                var prooflist = new AccountingProoflist
                                {
                                    CustomerId = customerNo,
                                    TransactionDate = transactionDate,
                                    OrderNo = worksheet.Cells[row, columnIndexes["order number"]].Value?.ToString(),
                                    NonMembershipFee = NonMembershipFee,
                                    PurchasedAmount = SubTotal,
                                    Amount = SubTotal + NonMembershipFee,
                                    StatusId = 3,
                                    StoreId = 0,
                                    FileDescriptionId = fileId,
                                    DeleteFlag = false,
                                };
                                pickARooProofList.Add(prooflist);
                            }
                        }
                    }
                }
                else
                {
                    // Find column indexes based on header names
                    for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                    {
                        var header = worksheet.Cells[1, col].Text.ToLower().Trim();
                        if (!string.IsNullOrEmpty(header))
                        {
                            columnIndexes[header] = col;
                        }
                    }

                    // Check if all expected headers exist in the first row
                    foreach (var expectedHeader in expectedHeadersFS)
                    {
                        if (!columnIndexes.ContainsKey(expectedHeader))
                        {
                            return (pickARooProofList, $"Column not found.");
                        }
                    }

                    DateTime date;
                    if (DateTime.TryParse(DateTime.Now.ToString(), out date))
                    {
                        var fileDescriptions = new FileDescriptions
                        {
                            FileName = fileName,
                            UploadDate = date,
                            Merchant = valueCust,
                            Count = rowCount,
                        };
                        _dbContext.FileDescription.Add(fileDescriptions);
                        await _dbContext.SaveChangesAsync();

                        fileId = fileDescriptions.Id;
                    }

                    for (row = 2; row <= rowCount; row++)
                    {
                        if (worksheet.Cells[row, columnIndexes["order number"]].Value != null ||
                            worksheet.Cells[row, columnIndexes["outlet name"]].Value != null ||
                            worksheet.Cells[row, columnIndexes["order delivery date"]].Value != null ||
                            worksheet.Cells[row, columnIndexes["total"]].Value != null)
                        {

                            var transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["order delivery date"]].Value);
                            decimal SubTotal = worksheet.Cells[row, columnIndexes["total"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["total"]].Value?.ToString()) : 0;
                            var chktransactionDate = new DateTime();
                            if (transactionDate.HasValue)
                            {
                                chktransactionDate = transactionDate.Value.Date;
                            }

                            if (transactionDate != null)
                            {
                                var prooflist = new AccountingProoflist
                                {
                                    CustomerId = customerNo,
                                    TransactionDate = transactionDate,
                                    OrderNo = worksheet.Cells[row, columnIndexes["order number"]].Value?.ToString(),
                                    NonMembershipFee = (decimal?)0.00,
                                    PurchasedAmount = (decimal?)0.00,
                                    Amount = SubTotal,
                                    StatusId = 3,
                                    StoreId = 0,
                                    FileDescriptionId = fileId,
                                    DeleteFlag = false,
                                };
                                pickARooProofList.Add(prooflist);
                            }
                        }
                    }
                }
                return (pickARooProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception)
            {
                return (pickARooProofList, "Error extracting proof list.");
            }
        }

        private async Task<(List<AccountingProoflist>, string?)> ExtractAccountingFoodPanda(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo)
        {
            var foodPandaProofList = new List<AccountingProoflist>();
            var fileId = 0;
            // Define expected headers
            string[] expectedHeaders = { "order code", "order date", "gross food value / product value" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            Dictionary<string, string> customers = new Dictionary<string, string>
            {
                {  "9999011929", "Grab Food" },
                {  "9999011955", "Grab Mart" },
                {  "9999011931", "Pick A Roo - Merch" },
                {  "9999011935", "Pick A Roo - FS" },
                {  "9999011838", "Food Panda" },
                {  "9999011855", "MetroMart" }
            };

            customers.TryGetValue(customerNo, out string valueCust);
            try
            {
                // Find column indexes based on header names
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                {
                    var header = worksheet.Cells[1, col].Text.ToLower().Trim();
                    if (!string.IsNullOrEmpty(header))
                    {
                        columnIndexes[header] = col;
                    }
                }

                // Check if all expected headers exist in the first row
                foreach (var expectedHeader in expectedHeaders)
                {
                    if (!columnIndexes.ContainsKey(expectedHeader))
                    {
                        return (foodPandaProofList, $"Column not found.");
                    }
                }

                DateTime date;
                if (DateTime.TryParse(DateTime.Now.ToString(), out date))
                {
                    var fileDescriptions = new FileDescriptions
                    {
                        FileName = fileName,
                        UploadDate = date,
                        Merchant = valueCust,
                        Count = rowCount,
                    };
                    _dbContext.FileDescription.Add(fileDescriptions);
                    await _dbContext.SaveChangesAsync();

                    fileId = fileDescriptions.Id;
                }

                for (row = 2; row <= rowCount; row++)
                {
                    if (worksheet.Cells[row, columnIndexes["order code"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["order date"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["gross food value / product value"]].Value != null)
                    {
                        var transactionDate = GetDateTimeFoodPanda(worksheet.Cells[row, columnIndexes["order date"]].Value);
                        decimal TotalPurchasedAmount = worksheet.Cells[row, columnIndexes["gross food value / product value"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["gross food value / product value"]].Value?.ToString()) : 0;
                        var chktransactionDate = new DateTime();
                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }

                        if (transactionDate != null)
                        {
                            var prooflist = new AccountingProoflist
                            {
                                CustomerId = customerNo,
                                TransactionDate = transactionDate,
                                OrderNo = worksheet.Cells[row, columnIndexes["order code"]].Value?.ToString(),
                                NonMembershipFee = (decimal?)0.00,
                                PurchasedAmount = (decimal?)0.00,
                                Amount = TotalPurchasedAmount,
                                StatusId = 3,
                                StoreId = 0,
                                FileDescriptionId = fileId,
                                DeleteFlag = false,
                            };
                            foodPandaProofList.Add(prooflist);
                        }
                    }
                }

                return (foodPandaProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception)
            {
                return (foodPandaProofList, "Error extracting proof list.");
            }
        }

        private async Task<(List<AccountingProoflist>, string?)> ExtractAccountingMetroMart(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo)
        {
            var metroMartProofList = new List<AccountingProoflist>();
            var fileId = 0;
            // Define expected headers
            string[] expectedHeaders = { "transaction date", "store name", "id", "total purchased amount"};

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();

            Dictionary<string, string> customers = new Dictionary<string, string>
            {
                {  "9999011929", "Grab Food" },
                {  "9999011955", "Grab Mart" },
                {  "9999011931", "Pick A Roo - Merch" },
                {  "9999011935", "Pick A Roo - FS" },
                {  "9999011838", "Food Panda" },
                {  "9999011855", "MetroMart" }
            };

            customers.TryGetValue(customerNo, out string valueCust);

            try
            {
                for (int col = 1; col <= worksheet.Dimension.Columns; col++)
                {
                    var header = worksheet.Cells[2, col].Text?.ToLower().Trim();
                    if (!string.IsNullOrWhiteSpace(header))
                    {
                        columnIndexes[header] = col;
                    }
                }

                // Check if all expected headers exist in the first row
                foreach (var expectedHeader in expectedHeaders)
                {
                    if (!columnIndexes.ContainsKey(expectedHeader))
                    {
                        return (metroMartProofList, $"Column not found.");
                    }
                }

                DateTime date;
                if (DateTime.TryParse(DateTime.Now.ToString(), out date))
                {
                    var fileDescriptions = new FileDescriptions
                    {
                        FileName = fileName,
                        UploadDate = date,
                        Merchant = valueCust,
                        Count = rowCount - 2,
                    };
                    _dbContext.FileDescription.Add(fileDescriptions);
                    await _dbContext.SaveChangesAsync();

                    fileId = fileDescriptions.Id;
                }

                for (row = 3; row <= rowCount; row++)
                {
                    if (worksheet.Cells[row, columnIndexes["transaction date"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["store name"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["id"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["total purchased amount"]].Value != null)
                    {

                        var transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["transaction date"]].Value);
                        var storeName = worksheet.Cells[row, columnIndexes["store name"]].Value?.ToString();
                        decimal TotalPurchasedAmount = worksheet.Cells[row, columnIndexes["total purchased amount"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["total purchased amount"]].Value?.ToString()) : 0;
                        var chktransactionDate = new DateTime();
                        var storedId = await ReturnLocation(storeName);
                        if (transactionDate.HasValue)
                        {
                            chktransactionDate = transactionDate.Value.Date;
                        }

                        if (transactionDate != null)
                        {
                            var prooflist = new AccountingProoflist
                            {
                                CustomerId = customerNo,
                                TransactionDate = transactionDate,
                                OrderNo = worksheet.Cells[row, columnIndexes["id"]].Value?.ToString(),
                                NonMembershipFee = (decimal?)0.00,
                                PurchasedAmount = (decimal?)0.00,
                                Amount = TotalPurchasedAmount,
                                StatusId = 3,
                                StoreId = storedId,
                                FileDescriptionId = fileId,
                                DeleteFlag = false,
                            };
                            metroMartProofList.Add(prooflist);
                        }
                    }
                }

                return (metroMartProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception)
            {
                return (metroMartProofList, "Error extracting proof list.");
            }
        }

        private async Task<int?> ReturnLocation(string location)
        {
            if (location != null || location != string.Empty)
            {
                string formatLocation = location.Replace("S&R New York Style Pizza - ", "");
                string removeText = formatLocation.Replace("[Available for LONG-DISTANCE DELIVERY]", "");
                string removeText1 = removeText.Replace("Libis Warehouse", "");
                string removeText2 = removeText1.Replace("Ilo-ilo Warehouse", "ILOILO");
                string removeText3 = removeText2.Replace("Evo City", "");
                string locationName = removeText3.Replace("S&R - ", "");
                string formatLocName = locationName.Replace("BGC", "FORT");
                string formatLocName1 = formatLocName.Replace("Libis", "C5");

                formatLocName1 = formatLocName1.Trim();

                var formatLocations = await _dbContext.Locations.Where(x => x.LocationName.ToLower().Contains("kareila"))
                    .ToListAsync();

                var getLocationCode =  formatLocations.Where(x => x.LocationName.ToLower().Contains(formatLocName1.ToLower()))
                .Select(n => n.LocationCode)
                .FirstOrDefault();

                return getLocationCode;
            }
            else
            {
                return null;
            }
        }

        public async Task<bool> DeleteAccountingAnalytics(int id)
        {
            var result = false;
            try
            {
                var fileDescDelete = await _dbContext.FileDescription
               .Where(x => x.Id == id)
               .ToListAsync();

                if (fileDescDelete != null)
                {
                    _dbContext.FileDescription.RemoveRange(fileDescDelete);
                    await _dbContext.SaveChangesAsync();
                }

                var accountingProoflistsDelete = await _dbContext.AccountingProoflists
                    .Where(x => x.FileDescriptionId == id)
                    .ToListAsync();

                if (accountingProoflistsDelete != null)
                {
                    _dbContext.AccountingProoflists.RemoveRange(accountingProoflistsDelete);
                    await _dbContext.SaveChangesAsync();
                }

                return true;
            }
            catch (Exception)
            {

                return false;
            }  
        }

        public async Task<List<PortalDto>> GetAccountingPortal(PortalParamsDto portalParamsDto)
        {
            try
            {
                var date1 = GetDateTime(portalParamsDto.dates[0].Date);
                var date2 = GetDateTime(portalParamsDto.dates[1].Date);
                var result = await _dbContext.AccountingProoflists
                    .Join(_dbContext.Locations, a => a.StoreId, b => b.LocationCode, (a, b) => new { a, b })
                    .Join(_dbContext.Status, c => c.a.StatusId, d => d.Id, (c, d) => new { c, d })
                    .Where(x => x.c.a.TransactionDate.Value.Date >= date1.Value.Date && x.c.a.TransactionDate.Value.Date <= date2.Value.Date
                        && x.c.a.CustomerId == portalParamsDto.memCode[0]
                        && x.c.a.StatusId != 4)
                    .Select(n => new PortalDto
                    {
                        Id = n.c.a.Id,
                        CustomerId = n.c.a.CustomerId,
                        TransactionDate = n.c.a.TransactionDate,
                        OrderNo = n.c.a.OrderNo,
                        NonMembershipFee = n.c.a.NonMembershipFee,
                        PurchasedAmount = n.c.a.PurchasedAmount,
                        Amount = n.c.a.Amount,
                        Status = n.d.StatusName,
                        StoreName = n.c.b.LocationName,
                        DeleteFlag = n.c.a.DeleteFlag
                    })
                    .ToListAsync();

                return result;
            }
            catch (Exception)
            {

                throw;
            }
        }

        public DateTime? GetDateTimeFoodPanda(object cellValue)
        {
            if (cellValue != null)
            {
                // Specify the format of the date string
                if (DateTime.TryParseExact(cellValue.ToString(), "dd.MM.yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var transactionDate))
                {
                    return transactionDate.Date;
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

    }
}
