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
                                StatusId = worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Cancelled" ? 4 : null,
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
                        var cnvrtDate = GetDateTime(selectedDate);
                        if (cnvrtDate == chktransactionDate)
                        {
                            var prooflist = new Prooflist
                            {
                                CustomerId = customerName == "PickARooMerch" ? "9999011931" : "9999011935",
                                TransactionDate = transactionDate,
                                OrderNo = worksheet.Cells[row, columnIndexes["order number"]].Value?.ToString(),
                                NonMembershipFee = (decimal?)0.00,
                                PurchasedAmount = (decimal?)0.00,
                                Amount = worksheet.Cells[row, columnIndexes["amount"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["amount"]].Value?.ToString()) : null,
                                StatusId = worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Cancelled" ? 4 : null,
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
                                        StatusId = worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["order status"]].Value?.ToString() == "Cancelled" && worksheet.Cells[row, columnIndexes["is payable"]].Value.ToString() == "yes" ? 3 : null,
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
                        var cnvrtDate = GetDateTime(selectedDate);
                        if (cnvrtDate == chktransactionDate)
                        {
                            var prooflist = new Prooflist
                            {
                                CustomerId = "9999011855",
                                TransactionDate = transactionDate,
                                OrderNo = worksheet.Cells[row, columnIndexes["jo #"]].Value?.ToString(),
                                NonMembershipFee = NonMembershipFee,
                                PurchasedAmount = PurchasedAmount,
                                Amount = NonMembershipFee + PurchasedAmount,
                                StatusId = worksheet.Cells[row, columnIndexes["jo delivery status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["jo delivery status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["jo delivery status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["jo delivery status"]].Value?.ToString() == "Cancelled" ? 4 : null,
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
                                StatusId = fields[columnIndexes["status"]] == "Completed" || fields[columnIndexes["status"]] == "Delivered" || fields[columnIndexes["status"]] == "Transferred" ? 3 : fields[columnIndexes["status"]] == "Cancelled" ? 4 : null,
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
                        if (cnvrtDate == chktransactionDate)
                        {
                            var metroMart = new Prooflist
                            {
                                CustomerId = "9999011855",
                                TransactionDate = transactionDate,
                                OrderNo = fields[columnIndexes["jo #"]],
                                NonMembershipFee = NonMembershipFee,
                                PurchasedAmount = PurchasedAmount,
                                Amount = amount,
                                StatusId = fields[columnIndexes["jo delivery status"]] == "Completed" || fields[columnIndexes["jo delivery status"]] == "Delivered" || fields[columnIndexes["jo delivery status"]] == "Transferred" ? 3 : fields[columnIndexes["jo delivery status"]] == "Cancelled" ? 4 : null,
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
                        var cnvrtDate = GetDateTime(selectedDate);
                        if (cnvrtDate == chktransactionDate)
                        {
                            var pickARoo = new Prooflist
                            {
                                CustomerId = customerName == "PickARooMerch" ? "9999011931" : "9999011935",
                                TransactionDate = fields[columnIndexes["order date"]].ToString() != "" ? GetDateTime(fields[columnIndexes["order date"]]) : null,
                                OrderNo = fields[columnIndexes["order number"]], 
                                NonMembershipFee = (decimal?)0.00,
                                PurchasedAmount = (decimal?)0.00,
                                Amount = fields[columnIndexes["amount"]] != "" ? decimal.Parse(fields[columnIndexes["amount"]]) : (decimal?)0.00, 
                                StatusId = fields[columnIndexes["order status"]] == "Completed" || fields[columnIndexes["order status"]] == "Delivered" || fields[columnIndexes["order status"]] == "Transferred" ? 3 : fields[columnIndexes["order status"]] == "Cancelled" ? 4 : null,
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
                                        StatusId = status == "completed" || status == "delivered" ? 3 : status == "cancelled" && isPayable == "yes" ? 3 : null,
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
    }
}
