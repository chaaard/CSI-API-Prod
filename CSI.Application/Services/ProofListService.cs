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
using SQLitePCL;
using NetTopologySuite.Geometries;
using System.Globalization;
using AutoMapper;
using NetTopologySuite.Index.HPRtree;
using System.Text.RegularExpressions;

namespace CSI.Application.Services
{
    public class ProofListService : IProofListService
    {
        private readonly AppDBContext _dbContext;
        private readonly IAnalyticsService _iAnalyticsService;
        private readonly IMapper _mapper;

        public ProofListService(AppDBContext dBContext, IAnalyticsService iAnalyticsService, IMapper mapper)
        {
            _dbContext = dBContext;
            _dbContext.Database.SetCommandTimeout(999);
            _iAnalyticsService = iAnalyticsService;
            _mapper = mapper;
        }

        public async Task<(List<Prooflist>?, string?)> ReadProofList(List<IFormFile> files, string customerName, string strClub, string selectedDate, string analyticsParamsDto)
        {
            int row = 2;
            int rowCount = 0;
            var club = Convert.ToInt32(strClub);
            var proofList = new List<Prooflist>();
            var param = new AnalyticsParamsDto();
            var rowsCountOld = 0;
            var rowsCountNew = 0;
            decimal totalAmount = 0;
            var fileName = "";

            foreach (var file in files)
            {
                fileName = file.FileName;
            }
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
            DateTime date1 = new DateTime();
            if (DateTime.TryParse(selectedDate, out date))
            {
                var GetAnalytics = await _dbContext.Analytics.Where(x => x.CustomerId.Contains(valueCust) && x.LocationId == club && x.TransactionDate == date).AnyAsync();
                if (!GetAnalytics)
                {
                    var logsDto = new LogsDto
                    {
                        UserId = param.userId,
                        Date = DateTime.Now,
                        Action = "Upload Prooflist",
                        Remarks = $"Error: No analytics found.",
                        Club = strClub,
                        Filename = fileName
                    };

                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

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
                                        var grabProofList = ExtractGrabMartOrFood(worksheet, fileName, rowCount, row, customerName, strClub, selectedDate, param.userId);
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
                                            if (grabProofList.Item1 != null)
                                            {
                                                proofList.AddRange(grabProofList.Item1);
                                            }
                                        }
                                    }
                                    else if (customerName == "PickARooFS" || customerName == "PickARooMerch")
                                    {
                                        var pickARooProofList = ExtractPickARoo(worksheet, fileName, rowCount, row, customerName, strClub, selectedDate, param.userId);
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
                                            if (pickARooProofList.Item1 != null)
                                            {
                                                proofList.AddRange(pickARooProofList.Item1);
                                            }
                                        }
                                    }
                                    else if (customerName == "FoodPanda")
                                    {
                                        var foodPandaProofList = ExtractFoodPanda(worksheet, fileName, rowCount, row, customerName, strClub, selectedDate, param.userId);
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
                                            if (foodPandaProofList.Item1 != null)
                                            {
                                                proofList.AddRange(foodPandaProofList.Item1);
                                            }
                                        }
                                    }
                                    else if (customerName == "MetroMart")
                                    {
                                        var metroMartProofList = ExtractMetroMart(worksheet, fileName, rowCount, row, customerName, strClub, selectedDate, param.userId);
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
                                            if (metroMartProofList.Item1 != null)
                                            {
                                                proofList.AddRange(metroMartProofList.Item1);
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    var logsDto = new LogsDto
                                    {
                                        UserId = param.userId,
                                        Date = DateTime.Now,
                                        Action = "Upload Prooflist",
                                        Remarks = $"Error: No worksheets found in the workbook.",
                                        Club = strClub,
                                        Filename = fileName
                                    };

                                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                    _dbContext.Logs.Add(logsMap);
                                    await _dbContext.SaveChangesAsync();

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
                                var grabProofList = ExtractCSVGrabMartOrFood(tempCsvFilePath, fileName, customerName, strClub, selectedDate, param.userId);
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
                                    if (grabProofList.Item1 != null)
                                    {
                                        proofList.AddRange(grabProofList.Item1);
                                    }
                                }
                            }
                            else if (customerName == "PickARooFS" || customerName == "PickARooMerch")
                            {
                                var pickARooProofList = ExtractCSVPickARoo(tempCsvFilePath, fileName, strClub, selectedDate, customerName, param.userId);
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
                                    if (pickARooProofList.Item1 != null)
                                    {
                                        proofList.AddRange(pickARooProofList.Item1);
                                    }
                                }
                            }
                            else if (customerName == "FoodPanda")
                            {
                                var foodPandaProofList = ExtractCSVFoodPanda(tempCsvFilePath, fileName, strClub, selectedDate, param.userId);
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
                                    if (foodPandaProofList.Item1 != null)
                                    {
                                        proofList.AddRange(foodPandaProofList.Item1);
                                    }
                                }
                            }
                            else if (customerName == "MetroMart")
                            {
                                var metroMartProofList = ExtractCSVMetroMart(tempCsvFilePath, fileName, strClub, selectedDate, param.userId);
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
                                    if (metroMartProofList.Item1 != null)
                                    {
                                        proofList.AddRange(metroMartProofList.Item1);
                                    }
                                }
                            }
                        }
                        else
                        {
                            var logsDto = new LogsDto
                            {
                                UserId = param.userId,
                                Date = DateTime.Now,
                                Action = "Upload Prooflist",
                                Remarks = $"Error: No worksheets found in the workbook.",
                                Club = strClub,
                                Filename = fileName
                            };

                            var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                            _dbContext.Logs.Add(logsMap);
                            await _dbContext.SaveChangesAsync();

                            return (null, "No worksheets found in the workbook.");
                        }
                    }
                }

                if (DataExistsInDatabase(proofList))
                {
                    customers.TryGetValue(customerName, out string value);
                    var convertDate = GetDateTime(selectedDate);
                    rowsCountOld = await DeleteRecords(club, convertDate, value);
                }

                if (proofList != null)
                {
                    await _dbContext.Prooflist.AddRangeAsync(proofList);
                    await _dbContext.SaveChangesAsync();
                    await _iAnalyticsService.UpdateUploadStatus(param);
                    rowsCountNew = proofList.Count;
                    totalAmount = proofList.Sum(x => x.Amount) ?? 0;

                    var logsDto = new LogsDto
                    {
                        UserId = param.userId,
                        Date = DateTime.Now,
                        Action = "Upload Prooflist",
                        Remarks = $"Success",
                        RowsCountBefore = rowsCountOld,
                        RowsCountAfter = rowsCountNew,
                        TotalAmount = totalAmount,
                        Club = strClub,
                        Filename = fileName
                    };

                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

                    return (proofList, "Success");
                }
                else
                {
                    var logsDto = new LogsDto
                    {
                        UserId = param.userId,
                        Date = DateTime.Now,
                        Action = "Upload Analytics",
                        Remarks = "Error: No list found.",
                        Club = strClub,
                        Filename = fileName
                    };

                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

                    return (proofList, "No list found.");
                }
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = param.userId,
                    Date = DateTime.Now,
                    Action = "Upload Analytics",
                    Remarks = $"Error: Please check error in row {row}: {ex.Message}",
                    Club = strClub,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return (null, $"Please check error in row {row}: {ex.Message}");
                throw;
            }
        }

        private async Task<int> DeleteRecords(int club, DateTime? selectedDate, string customerId)
        {
            var rowsCount = 0;
            var dataToDelete = _dbContext.Prooflist
                .Where(x => x.CustomerId.Contains(customerId) && x.TransactionDate == selectedDate && x.StoreId == club)
                .ToList();

            rowsCount = dataToDelete.Count;

            if (dataToDelete != null)
            {
                _dbContext.Prooflist.RemoveRange(dataToDelete);
                _dbContext.SaveChanges();
            }

            return rowsCount;
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

        private (List<Prooflist>, string?) ExtractGrabMartOrFood(ExcelWorksheet worksheet, string fileName, int rowCount, int row, string customerName, string strClub, string selectedDate, string userId)
        {
            var getLocation = _dbContext.Locations.ToList();
            var grabFoodProofList = new List<Prooflist>();
            DateTime date1 = new DateTime();
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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Analytics",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        _dbContext.SaveChangesAsync();

                        return (grabFoodProofList, $"Column not found.");
                    }
                }

                var merchantName = worksheet.Cells[2, columnIndexes["type"]].Value?.ToString();

                if (merchantName != customerName)
                {
                    var logsDto = new LogsDto
                    {
                        UserId = userId,
                        Date = DateTime.Now,
                        Action = "Upload Analytics",
                        Remarks = $"Error: Uploaded file merchant do not match.",
                        Club = strClub,
                        Filename = fileName
                    }; 
                    
                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    _dbContext.SaveChangesAsync();

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
                                StoreId = Convert.ToInt32(strClub),
                                DeleteFlag = false,
                            };
                            grabFoodProofList.Add(prooflist);
                        }
                        else
                        {
                            var logsDto = new LogsDto
                            {
                                UserId = userId,
                                Date = DateTime.Now,
                                Action = "Upload Analytics",
                                Remarks = $"Error: Uploaded file transaction dates do not match.",
                                Club = strClub,
                                Filename = fileName
                            };

                            var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                            _dbContext.Logs.Add(logsMap);
                            _dbContext.SaveChangesAsync();

                            return (grabFoodProofList, "Uploaded file transaction dates do not match.");
                        }
                    }
                }

                return (grabFoodProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Analytics",
                    Remarks = $"Error: {ex.Message}",
                    Club = strClub,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                _dbContext.SaveChangesAsync();

                return (grabFoodProofList, "Error extracting proof list.");
            }
        }

        private (List<Prooflist>, string?) ExtractPickARoo(ExcelWorksheet worksheet, string fileName, int rowCount, int row, string customerName, string strClub, string selectedDate, string userId)
        {
            var pickARooProofList = new List<Prooflist>();
            // Define expected headers
            string[] expectedHeaders = { "order date", "order number", "order status", "amount" };
            DateTime date1 = new DateTime();
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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Analytics",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        _dbContext.SaveChangesAsync();

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
                                        StoreId = Convert.ToInt32(strClub),
                                        DeleteFlag = false,
                                    };
                                    pickARooProofList.Add(prooflist);
                                }
                                else
                                {
                                    var logsDto = new LogsDto
                                    {
                                        UserId = userId,
                                        Date = DateTime.Now,
                                        Action = "Upload Analytics",
                                        Remarks = $"Error: Uploaded file transaction dates do not match.",
                                        Club = strClub,
                                        Filename = fileName
                                    };

                                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                    _dbContext.Logs.Add(logsMap);
                                    _dbContext.SaveChangesAsync();

                                    return (pickARooProofList, "Uploaded file transaction dates do not match.");
                                }
                            }
                        }
                    }
                }

                return (pickARooProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Analytics",
                    Remarks = $"Error: {ex.Message} ",
                    Club = strClub,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                _dbContext.SaveChangesAsync();

                return (pickARooProofList, "Error extracting proof list.");
            }
        }

        private (List<Prooflist>, string?) ExtractFoodPanda(ExcelWorksheet worksheet, string fileName, int rowCount, int row, string customerName, string strClub, string selectedDate, string userId)
        {
            var foodPandaProofList = new List<Prooflist>();
            var transactionDate = new DateTime?();
            // Define expected headers
            string[] expectedHeaders = { "order id", "order status", "delivered at", "subtotal", "cancelled at", "is payable" };
            DateTime date1 = new DateTime();
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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Analytics",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        _dbContext.SaveChangesAsync();

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
                                        StoreId = Convert.ToInt32(strClub),
                                        DeleteFlag = false,
                                    };
                                    foodPandaProofList.Add(prooflist);
                                }
                                else
                                {
                                    var logsDto = new LogsDto
                                    {
                                        UserId = userId,
                                        Date = DateTime.Now,
                                        Action = "Upload Analytics",
                                        Remarks = $"Error: Uploaded file transaction dates do not match.",
                                        Club = strClub,
                                        Filename = fileName
                                    };

                                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                    _dbContext.Logs.Add(logsMap);
                                    _dbContext.SaveChangesAsync();

                                    return (foodPandaProofList, "Uploaded file transaction dates do not match.");
                                }
                            }
                        }
                    }
                }

                return (foodPandaProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Analytics",
                    Remarks = $"Error: {ex.Message} ",
                    Club = strClub,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                _dbContext.SaveChangesAsync();

                return (foodPandaProofList, "Error extracting proof list.");
            }
        }

        private (List<Prooflist>, string?) ExtractMetroMart(ExcelWorksheet worksheet, string fileName, int rowCount, int row, string customerName, string strClub, string selectedDate, string userId)
        {
            var metroMartProofList = new List<Prooflist>();
            // Define expected headers
            string[] expectedHeaders = { "jo #", "jo delivery status", "completed date", "non membership fee", "purchased amount" };
            DateTime date1 = new DateTime();
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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Analytics",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        _dbContext.SaveChangesAsync();

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
                                        StoreId = Convert.ToInt32(strClub),
                                        DeleteFlag = false,
                                    };
                                    metroMartProofList.Add(prooflist);
                                }
                                else
                                {
                                    var logsDto = new LogsDto
                                    {
                                        UserId = userId,
                                        Date = DateTime.Now,
                                        Action = "Upload Analytics",
                                        Remarks = $"Error: Uploaded file transaction dates do not match.",
                                        Club = strClub,
                                        Filename = fileName
                                    };

                                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                    _dbContext.Logs.Add(logsMap);
                                    _dbContext.SaveChangesAsync();

                                    return (metroMartProofList, "Uploaded file transaction dates do not match.");
                                }
                            }
                        }
                    }
                }

                return (metroMartProofList, rowCount.ToString() + " rows extracted");
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Analytics",
                    Remarks = $"Error: {ex.Message} ",
                    Club = strClub,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                _dbContext.SaveChangesAsync();

                return (metroMartProofList, "Error extracting proof list.");
            }
        }
        
        private (List<Prooflist>, string?) ExtractCSVGrabMartOrFood(string filePath, string fileName, string customerName, string strClub, string selectedDate, string userId)
        {
            int row = 2;
            int rowCount = 0;
            var grabFoodProofLists = new List<Prooflist>();
            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            DateTime date1 = new DateTime();
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
                                var logsDto = new LogsDto
                                {
                                    UserId = userId,
                                    Date = DateTime.Now,
                                    Action = "Upload Analytics",
                                    Remarks = $"Error: Column not found.",
                                    Club = strClub,
                                    Filename = fileName
                                };

                                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                _dbContext.Logs.Add(logsMap);
                                _dbContext.SaveChangesAsync();

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
                                StoreId = Convert.ToInt32(strClub),
                                DeleteFlag = false,
                            };

                            grabFoodProofLists.Add(grabfood);
                            rowCount++;
                        }
                        else
                        {
                            var logsDto = new LogsDto
                            {
                                UserId = userId,
                                Date = DateTime.Now,
                                Action = "Upload Analytics",
                                Remarks = $"Error: Uploaded file transaction dates do not match.",
                                Club = strClub,
                                Filename = fileName
                            };

                            var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                            _dbContext.Logs.Add(logsMap);
                            _dbContext.SaveChangesAsync();

                            return (grabFoodProofLists, "Uploaded file transaction dates do not match.");
                        }
                    }
                }

                return (grabFoodProofLists, rowCount.ToString() + " rows extracted");
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Analytics",
                    Remarks = $"Error: {ex.Message} ",
                    Club = strClub,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                _dbContext.SaveChangesAsync();

                return (grabFoodProofLists, $"Please check error in row {rowCount}: {ex.Message}");
            }
        }

        private (List<Prooflist>, string?) ExtractCSVMetroMart(string filePath, string fileName, string strClub, string selectedDate, string userId)
        {
            int row = 2;
            int rowCount = 0;
            var metroMartProofLists = new List<Prooflist>();
            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            DateTime date1 = new DateTime();
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
                                var logsDto = new LogsDto
                                {
                                    UserId = userId,
                                    Date = DateTime.Now,
                                    Action = "Upload Analytics",
                                    Remarks = $"Error: Column not found.",
                                    Club = strClub,
                                    Filename = fileName
                                };

                                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                _dbContext.Logs.Add(logsMap);
                                _dbContext.SaveChangesAsync();

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
                                        StoreId = Convert.ToInt32(strClub),
                                        DeleteFlag = false,
                                    };

                                    metroMartProofLists.Add(metroMart);
                                    rowCount++;
                                }
                                else
                                {
                                    var logsDto = new LogsDto
                                    {
                                        UserId = userId,
                                        Date = DateTime.Now,
                                        Action = "Upload Analytics",
                                        Remarks = $"Error: Uploaded file transaction dates do not match.",
                                        Club = strClub,
                                        Filename = fileName
                                    };

                                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                    _dbContext.Logs.Add(logsMap);
                                    _dbContext.SaveChangesAsync();

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
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Analytics",
                    Remarks = $"Error: {ex.Message} ",
                    Club = strClub,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                _dbContext.SaveChangesAsync();

                return (metroMartProofLists, $"Please check error in row {row}: {ex.Message}");
            }
        }

        private (List<Prooflist>, string?) ExtractCSVPickARoo(string filePath, string fileName, string strClub, string selectedDate, string customerName, string userId)
        {
            int row = 2;
            int rowCount = 0;
            var pickARooProofLists = new List<Prooflist>();
            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            DateTime date1 = new DateTime();
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
                                var logsDto = new LogsDto
                                {
                                    UserId = userId,
                                    Date = DateTime.Now,
                                    Action = "Upload Analytics",
                                    Remarks = $"Error: Column not found.",
                                    Club = strClub,
                                    Filename = fileName
                                };

                                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                _dbContext.Logs.Add(logsMap);
                                _dbContext.SaveChangesAsync();

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
                                        StoreId = Convert.ToInt32(strClub),
                                        DeleteFlag = false,
                                    };

                                    pickARooProofLists.Add(pickARoo);
                                    rowCount++;
                                }
                                else
                                {
                                    var logsDto = new LogsDto
                                    {
                                        UserId = userId,
                                        Date = DateTime.Now,
                                        Action = "Upload Analytics",
                                        Remarks = $"Error: Uploaded file transaction dates do not match.",
                                        Club = strClub,
                                        Filename = fileName
                                    };

                                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                    _dbContext.Logs.Add(logsMap);
                                    _dbContext.SaveChangesAsync();

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
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Analytics",
                    Remarks = $"Error: {ex.Message} ",
                    Club = strClub,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                _dbContext.SaveChangesAsync();

                return (pickARooProofLists, $"Please check error in row {rowCount}: {ex.Message}");
            }
        }

        private (List<Prooflist>, string?) ExtractCSVFoodPanda(string filePath, string fileName, string strClub, string selectedDate, string userId)
        {
            int row = 2;
            int rowCount = 0;
            var foodPandaProofLists = new List<Prooflist>();
            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            DateTime date1 = new DateTime();
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
                                var logsDto = new LogsDto
                                {
                                    UserId = userId,
                                    Date = DateTime.Now,
                                    Action = "Upload Analytics",
                                    Remarks = $"Error: Column not found.",
                                    Club = strClub,
                                    Filename = fileName
                                };

                                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                _dbContext.Logs.Add(logsMap);
                                _dbContext.SaveChangesAsync();

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
                                        StoreId = Convert.ToInt32(strClub),
                                        DeleteFlag = false,
                                    };

                                    foodPandaProofLists.Add(foodPanda);
                                    rowCount++;
                                }
                                else
                                {
                                    var logsDto = new LogsDto
                                    {
                                        UserId = userId,
                                        Date = DateTime.Now,
                                        Action = "Upload Analytics",
                                        Remarks = $"Error: Uploaded file transaction dates do not match.",
                                        Club = strClub,
                                        Filename = fileName
                                    };

                                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                    _dbContext.Logs.Add(logsMap);
                                    _dbContext.SaveChangesAsync();

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
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Analytics",
                    Remarks = $"Error: {ex.Message} ",
                    Club = strClub,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                _dbContext.SaveChangesAsync();

                return (foodPandaProofLists, $"Please check error in row {row}: {ex.Message}");
            }
        }

        public int? GetLocationId(string? location, List<Domain.Entities.Location> locations)
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

        public async Task<(List<AccountingProoflist>?, string?)> ReadAccountingProofList(List<IFormFile> files, string customerName, string userId, string strClub)
        {
            int row = 2;
            int rowCount = 0;
            int rowCountAdj = 0;
            var proofList = new List<AccountingProoflist>();
            var proofListAdj = new List<AccountingProoflistAdjustments>();
            var rowsCountOld = 0;
            var rowsCountNew = 0;
            decimal totalAmount = 0;
            var fileName = "";

            foreach (var getFile in files)
            {
                fileName = getFile.FileName;
            }

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
                                   
                                    // Check if the filename contains the word "grabfood"
                                    if (valueCust == "GrabMart" || valueCust == "GrabFood")
                                    {
                                        worksheet = package.Workbook.Worksheets[1];
                                        rowCount = worksheet.Dimension.Rows;
                                        var grabProofList = await ExtractAccountingGrabMartOrFood(worksheet, rowCount, row, file.FileName.ToString(), customerName, strClub, userId);
                                        
                                        var grabProofListAdj = await ExtractAccountingGrabMartOrFoodAdjustments(worksheet, rowCount, row, file.FileName.ToString(), customerName, strClub, userId, grabProofList.Item3, grabProofList.Item4);
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
                                            if (grabProofList.Item1 != null)
                                            {
                                                proofList.AddRange(grabProofList.Item1);
                                            }

                                            if (grabProofListAdj != null)
                                            {
                                                proofListAdj.AddRange(grabProofListAdj);
                                            }
                                        }
                                    }
                                    else if (valueCust == "PickARooFS" || valueCust == "PickARooMerch")
                                    {
                                        worksheet = package.Workbook.Worksheets[0];
                                        rowCount = worksheet.Dimension.Rows;
                                        var pickARooProofList = await ExtractAccountingPickARoo(worksheet, rowCount, row, file.FileName.ToString(), customerName, strClub, userId);
                                        var pickARooProofListAdj = new List<AccountingProoflistAdjustments>();
                                        if (package.Workbook.Worksheets.Count == 2)
                                        {
                                            var worksheetAdj = package.Workbook.Worksheets[1];
                                            rowCountAdj = worksheetAdj.Dimension.Rows;
                                            pickARooProofListAdj = await ExtractAccountingPickARooAdjustments(worksheetAdj, rowCountAdj, row, file.FileName.ToString(), customerName, strClub, userId, pickARooProofList.Item3);
                                        }
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
                                            if (pickARooProofList.Item1 != null)
                                            {
                                                proofList.AddRange(pickARooProofList.Item1);
                                            }

                                            if (pickARooProofListAdj != null)
                                            {
                                                proofListAdj.AddRange(pickARooProofListAdj);
                                            }
                                        }
                                    }
                                    else if (valueCust == "FoodPanda")
                                    {
                                        worksheet = package.Workbook.Worksheets[0];
                                        rowCount = worksheet.Dimension.Rows;
                                        var foodPandaProofList = await ExtractAccountingFoodPanda(worksheet, rowCount, row, file.FileName.ToString(), customerName, strClub, userId);
                                        var foodPandaProofListAdj = new List<AccountingProoflistAdjustments>();
                                        if (package.Workbook.Worksheets.Count == 2)
                                        {
                                            var worksheetAdj = package.Workbook.Worksheets[1];
                                            rowCountAdj = worksheetAdj.Dimension.Rows;
                                            foodPandaProofListAdj = await ExtractAccountingFoodPandaAdjustments(worksheetAdj, rowCountAdj, row, file.FileName.ToString(), customerName, strClub, userId, foodPandaProofList.Item3);
                                        }
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
                                            if (foodPandaProofList.Item1 != null)
                                            {
                                                proofList.AddRange(foodPandaProofList.Item1);
                                            }

                                            if (foodPandaProofListAdj != null)
                                            {
                                                proofListAdj.AddRange(foodPandaProofListAdj);
                                            }
                                        }
                                    }
                                    else if (valueCust == "MetroMart")
                                    {
                                        worksheet = package.Workbook.Worksheets[0];
                                        rowCount = worksheet.Dimension.Rows;
                                        var metroMartProofList = await ExtractAccountingMetroMart(worksheet, rowCount, row, file.FileName.ToString(), customerName, strClub, userId);
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
                                            if (metroMartProofList.Item1 != null)
                                            {
                                                proofList.AddRange(metroMartProofList.Item1);
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    var logsDto = new LogsDto
                                    {
                                        UserId = userId,
                                        Date = DateTime.Now,
                                        Action = "Upload Accounting Prooflist",
                                        Remarks = $"No worksheets found in the workbook.",
                                        Club = strClub,
                                        CustomerId = customerName,
                                        Filename = fileName
                                    };

                                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                                    _dbContext.Logs.Add(logsMap);
                                    await _dbContext.SaveChangesAsync();

                                    return (null, "No worksheets found in the workbook.");
                                }
                            }
                        }
                        else
                        {
                            var logsDto = new LogsDto
                            {
                                UserId = userId,
                                Date = DateTime.Now,
                                Action = "Upload Accounting Prooflist",
                                Remarks = $"No worksheets.",
                                Club = strClub,
                                CustomerId = customerName,
                                Filename = fileName
                            };

                            var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                            _dbContext.Logs.Add(logsMap);
                            await _dbContext.SaveChangesAsync();

                            return (null, "No worksheets.");
                        }
                    }
                }

                if (proofList != null)
                {
                    if (valueCust == "FoodPanda")
                    {
                        var locations = await _dbContext.Locations.ToListAsync();

                        foreach (var proof in proofList)
                        {
                            foreach (var location in locations)
                            {
                                if (!string.IsNullOrEmpty(proof.OrderNo) && !string.IsNullOrEmpty(location.VendorCode) && proof.OrderNo.Contains(location.VendorCode))
                                {
                                    proof.StoreId = location.Id;
                                    break;
                                }
                            }
                        }
                    }

                    await _dbContext.AccountingProoflists.AddRangeAsync(proofList);
                    await _dbContext.SaveChangesAsync();

                    var savedProofLists = _dbContext.AccountingProoflists
                        .Where(pl => proofList.Select(p => p.Id).Contains(pl.Id) && pl.StatusId == 3)
                        .ToList();


                    if (savedProofLists.Where(x => x.CustomerId == "9999011935" || x.CustomerId == "9999011931").Any())
                    {
                        foreach (var proof in savedProofLists)
                        {
                            var matchedAnalytics = _dbContext.AccountingAnalytics
                                .Where(aa => aa.OrderNo == proof.OrderNo
                                             && aa.TransactionDate == proof.TransactionDate
                                             && aa.LocationId == proof.StoreId)
                                .FirstOrDefault();

                            if (matchedAnalytics != null)
                            {
                                var accountingMatch = _dbContext.AccountingMatchPayment
                                    .Where(am => am.AccountingAnalyticsId == matchedAnalytics.Id)
                                    .FirstOrDefault();

                                int accountingStatusId = (matchedAnalytics.SubTotal == null) ? 4 :
                                                          (proof.Amount == null) ? 5 :
                                                          matchedAnalytics.SubTotal == proof.Amount ? 1 :
                                                          matchedAnalytics.SubTotal > proof.Amount ? 2 :
                                                          matchedAnalytics.SubTotal < proof.Amount ? 3 : 4;

                                if (accountingMatch != null)
                                {
                                    accountingMatch.AccountingProofListId = proof.Id;
                                    accountingMatch.AccountingStatusId = accountingStatusId;
                                }
                                else
                                {
                                    _dbContext.AccountingMatchPayment.Add(new AccountingMatchPayment
                                    {
                                        AccountingAnalyticsId = matchedAnalytics.Id,
                                        AccountingProofListId = proof.Id,
                                        AccountingStatusId = accountingStatusId
                                    });
                                }
                            }
                            else
                            {
                                int accountingStatusId = 4;

                                _dbContext.AccountingMatchPayment.Add(new AccountingMatchPayment
                                {
                                    AccountingAnalyticsId = null,
                                    AccountingProofListId = proof.Id,
                                    AccountingStatusId = accountingStatusId
                                });
                            }
                        }
                    }
                    else
                    {
                        foreach (var proof in savedProofLists)
                        {
                            var matchedAnalytics = _dbContext.AccountingAnalytics
                                .Where(aa => aa.OrderNo == proof.OrderNo
                                             && aa.TransactionDate == proof.TransactionDate
                                             && aa.LocationId == proof.StoreId)
                                .FirstOrDefault();

                            if (matchedAnalytics != null)
                            {
                                var accountingMatch = _dbContext.AccountingMatchPayment
                                    .Where(am => am.AccountingAnalyticsId == matchedAnalytics.Id)
                                    .FirstOrDefault();

                                int accountingStatusId;

                                if (matchedAnalytics.SubTotal == null)
                                {
                                    accountingStatusId = 4;
                                }
                                else if (proof.Amount == null)
                                {
                                    accountingStatusId = 5;
                                }
                                else
                                {
                                    var subTotal = matchedAnalytics.SubTotal ?? 0;
                                    var amount = proof.Amount ?? 0;
                                    var difference = subTotal - amount;
 
                                    if (difference <= 1.0M && difference >= -1.0M)
                                    {
                                        accountingStatusId = 1;
                                    }
                                    else
                                    {
                                        accountingStatusId = subTotal == amount ? 1 :
                                                             subTotal > amount ? 2 :
                                                             subTotal < amount ? 3 : 4;
                                    }
                                }

                                if (accountingMatch != null)
                                {
                                    accountingMatch.AccountingProofListId = proof.Id;
                                    accountingMatch.AccountingStatusId = accountingStatusId;
                                }
                                else
                                {
                                    _dbContext.AccountingMatchPayment.Add(new AccountingMatchPayment
                                    {
                                        AccountingAnalyticsId = matchedAnalytics.Id,
                                        AccountingProofListId = proof.Id,
                                        AccountingStatusId = accountingStatusId
                                    });
                                }
                            }
                            else
                            {
                                int accountingStatusId = 4;

                                _dbContext.AccountingMatchPayment.Add(new AccountingMatchPayment
                                {
                                    AccountingAnalyticsId = null,
                                    AccountingProofListId = proof.Id,
                                    AccountingStatusId = accountingStatusId
                                });
                            }
                        }
                    }

                    await _dbContext.SaveChangesAsync();

                    if (proofListAdj != null)
                    {
                        await _dbContext.AccountingProoflistAdjustments.AddRangeAsync(proofListAdj);
                        await _dbContext.SaveChangesAsync();


                        if (proofListAdj.Any(x => x.CustomerId.Contains("9999011838")))
                        {
                            //Use in adjustment FoodPanda
                            var formatProofListAdj = proofListAdj.Where(x => x.CustomerId == "9999011838").ToList();

                            foreach (var item in formatProofListAdj)
                            {
                                var getAdjustment = await _dbContext.AccountingProoflists
                                    .FirstOrDefaultAsync(x => x.CustomerId == "9999011838" && x.OrderNo == item.OrderNo);

                                if (getAdjustment != null)
                                {

                                    if (item.Amount < 0)
                                    {
                                        getAdjustment.Amount += item.Amount;
                                    }
                                    else
                                    {
                                        getAdjustment.Amount -= item.Amount;
                                    }


                                    _dbContext.AccountingProoflists.Update(getAdjustment);
                                    await _dbContext.SaveChangesAsync();

                                    var accountingMatch = await _dbContext.AccountingMatchPayment
                                        .FirstOrDefaultAsync(am => am.AccountingProofListId == getAdjustment.Id);

                                    if (accountingMatch != null)
                                    {
                                        if (accountingMatch.AccountingStatusId == 1)
                                        {
                                            accountingMatch.AccountingStatusId = 20;
                                            _dbContext.AccountingMatchPayment.Update(accountingMatch);
                                            await _dbContext.SaveChangesAsync();
                                        }
                                        else
                                        {
                                            var getAccountingAnalytics = await _dbContext.AccountingAnalytics
                                           .FirstOrDefaultAsync(am => am.Id == accountingMatch.AccountingAnalyticsId);

                                            var aAmount = getAccountingAnalytics?.SubTotal ?? 0;
                                            var plAmount = getAdjustment.Amount;

                                            int accountingStatusId = aAmount == plAmount ? 17 :
                                                                     aAmount > plAmount ? 18 :
                                                                     aAmount < plAmount ? 19 : 4;

                                            accountingMatch.AccountingStatusId = accountingStatusId;
                                            _dbContext.AccountingMatchPayment.Update(accountingMatch);
                                            await _dbContext.SaveChangesAsync();
                                        }
                                    }
                                }
                            }
                        }
                        else if (proofListAdj.Any(x => x.CustomerId.Contains("9999011955") || x.CustomerId.Contains("9999011929")))
                        {
                            //Use in adjustment GrabFood & GrabMart
                        }
                        else if (proofListAdj.Any(x => x.CustomerId.Contains("9999011935") || x.CustomerId.Contains("9999011931")))
                        {
                            //Use in adjustment Pick A Roo FS / Merch
                            var formatProofListAdj = proofListAdj.Where(x => x.CustomerId == "9999011935" || x.CustomerId == "9999011931").ToList();

                            foreach (var item in formatProofListAdj)
                            {
                                var getAdjustment = await _dbContext.AccountingProoflists
                                    .FirstOrDefaultAsync(x => x.CustomerId == "9999011935" && x.OrderNo == item.OrderNo || x.CustomerId == "9999011931" && x.OrderNo == item.OrderNo);

                                if (getAdjustment != null)
                                {

                                    if (item.Amount > 0)
                                    {
                                        getAdjustment.Amount += item.Amount;
                                    }
                                    else
                                    {
                                        getAdjustment.Amount -= item.Amount;
                                    }

                                    _dbContext.AccountingProoflists.Update(getAdjustment);
                                    await _dbContext.SaveChangesAsync();

                                    var accountingMatch = await _dbContext.AccountingMatchPayment
                                        .FirstOrDefaultAsync(am => am.AccountingProofListId == getAdjustment.Id);

                                    if (accountingMatch != null)
                                    {
                                        if (accountingMatch.AccountingStatusId == 1)
                                        {
                                            accountingMatch.AccountingStatusId = 20;
                                            _dbContext.AccountingMatchPayment.Update(accountingMatch);
                                            await _dbContext.SaveChangesAsync();
                                        }
                                        else
                                        {
                                            var getAccountingAnalytics = await _dbContext.AccountingAnalytics
                                                .FirstOrDefaultAsync(am => am.Id == accountingMatch.AccountingAnalyticsId);

                                            var aAmount = getAccountingAnalytics?.SubTotal ?? 0;
                                            var plAmount = getAdjustment.Amount;

                                            int accountingStatusId = aAmount == plAmount ? 17 :
                                                                     aAmount > plAmount ? 18 :
                                                                     aAmount < plAmount ? 19 : 4;

                                            accountingMatch.AccountingStatusId = accountingStatusId;
                                            _dbContext.AccountingMatchPayment.Update(accountingMatch);
                                            await _dbContext.SaveChangesAsync();
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            //MetroMart
                            //Do Nothing Here Since MetroMart Doesn't Have Adjustments
                            
                        }
                    }

                    rowsCountNew = proofList.Count;
                    totalAmount = proofList.Sum(x => x.Amount) ?? 0;
                    var logsDto = new LogsDto
                    {
                        UserId = userId,
                        Date = DateTime.Now,
                        Action = "Upload Accounting Prooflist",
                        Remarks = $"Success",
                        RowsCountAfter = rowsCountNew,
                        TotalAmount = totalAmount,
                        Club = strClub,
                        CustomerId = customerName,
                        Filename = fileName
                    };

                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();
                    return (proofList, "Success");
                }
                else
                {
                    var logsDto = new LogsDto
                    {
                        UserId = userId,
                        Date = DateTime.Now,
                        Action = "Upload Accounting Prooflist",
                        Remarks = $"No list found.",
                        Club = strClub,
                        CustomerId = customerName,
                        Filename = fileName
                    };

                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

                    return (proofList, "No list found.");
                }
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Accounting Analytics",
                    Remarks = $"Error: Please check error in row {row}: {ex.Message}",
                    Club = strClub,
                    CustomerId = customerName,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return (null, $"Please check error in row {row}: {ex.Message}");
                throw;
            }
        }

        private async Task<(List<AccountingProoflist>, string?, int?, int?)> ExtractAccountingGrabMartOrFood(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo, string strClub, string userId)
        {
            var grabFoodProofList = new List<AccountingProoflist>();
            var fileIdGM = 0;
            var fileIdGF = 0;
            // Define expected headers
            string[] expectedHeaders = { "updated on", "store name", "short order id", "net sales", "channel commission", "status" };

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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Accounting Analytics",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            CustomerId = customerNo,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        await _dbContext.SaveChangesAsync();

                        return (grabFoodProofList, $"Column not found.", 0, 0);
                    }
                }

                for (row = 2; row <= rowCount; row++)
                {
                    if (worksheet.Cells[row, columnIndexes["updated on"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["store name"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["short order id"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["channel commission"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["net sales"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["status"]].Value != null)
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
                                    StatusId = worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Cancelled" ? 4 : 3,
                                    StoreId = await ReturnLocation(storeName),
                                    FileDescriptionId = fileIdGF,
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
                                    StatusId = worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Completed" || worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Delivered" || worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Transferred" ? 3 : worksheet.Cells[row, columnIndexes["status"]].Value?.ToString() == "Cancelled" ? 4 : 3,
                                    StoreId = await ReturnLocation(storeName),
                                    FileDescriptionId = fileIdGM,
                                    AgencyFee = agencyfee,
                                    DeleteFlag = false,
                                };
                                grabFoodProofList.Add(prooflist);
                            }
                        }
                    }
                }
                if (grabFoodProofList.Any(x => x.CustomerId == "9999011929"))
                {
                    DateTime date1;
                    if (DateTime.TryParse(DateTime.Now.ToString(), out date1))
                    {
                        var fileDescriptions = new FileDescriptions
                        {
                            FileName = fileName,
                            UploadDate = date1,
                            Merchant = "Grab Food",
                            Count = grabFoodProofList.Count(x => x.CustomerId == "9999011929"),
                        };
                        _dbContext.FileDescription.Add(fileDescriptions);
                        await _dbContext.SaveChangesAsync();

                        var getOnlyGrabFood = grabFoodProofList
                            .Where(x => x.CustomerId == "9999011929")
                            .ToList();

                        foreach (var item in getOnlyGrabFood)
                        {
                            item.FileDescriptionId = fileDescriptions.Id;
                        }

                        fileIdGF = getOnlyGrabFood.Select(x => x.FileDescriptionId).FirstOrDefault() ?? 0;
                    }
                }

                if (grabFoodProofList.Any(x => x.CustomerId == "9999011955"))
                {
                    DateTime date1;
                    if (DateTime.TryParse(DateTime.Now.ToString(), out date1))
                    {
                        var fileDescriptions = new FileDescriptions
                        {
                            FileName = fileName,
                            UploadDate = date1,
                            Merchant = "Grab Mart",
                            Count = grabFoodProofList.Count(x => x.CustomerId == "9999011955"),
                        };
                        _dbContext.FileDescription.Add(fileDescriptions);
                        await _dbContext.SaveChangesAsync();

                        var getOnlyGrabMart = grabFoodProofList
                           .Where(x => x.CustomerId == "9999011955")
                           .ToList();

                        foreach (var item in getOnlyGrabMart)
                        {
                            item.FileDescriptionId = fileDescriptions.Id;
                        }

                        fileIdGM = getOnlyGrabMart.Select(x => x.FileDescriptionId).FirstOrDefault() ?? 0;
                    }
                }

                return (grabFoodProofList, rowCount.ToString() + " rows extracted", fileIdGF, fileIdGM);
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Accounting Analytics",
                    Remarks = $"Error: Please check error in row {row}: {ex.Message}",
                    Club = strClub,
                    CustomerId = customerNo,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return (grabFoodProofList, "Error extracting proof list.", 0, 0);
            }
        }

        private async Task<List<AccountingProoflistAdjustments>> ExtractAccountingGrabMartOrFoodAdjustments(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo, string strClub, string userId, int? fileIdGF, int? fileIdGM)
        {
            var grabFoodProofListAdj = new List<AccountingProoflistAdjustments>();
            // Define expected headers
            string[] expectedHeaders = { "updated on", "store name", "short order id", "total", "description" };

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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Accounting Analytics Adjustments",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            CustomerId = customerNo,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        await _dbContext.SaveChangesAsync();

                        return grabFoodProofListAdj;
                    }
                }

                for (row = 2; row <= rowCount; row++)
                {
                    if (worksheet.Cells[row, columnIndexes["description"]].Value != null)
                    {
                        var orderNo = worksheet.Cells[row, columnIndexes["short order id"]].Value?.ToString();
                        var description = worksheet.Cells[row, columnIndexes["description"]].Value?.ToString();
                        if (orderNo.Contains("GF"))
                        {
                            var transactionDate = GetDateTime(worksheet.Cells[row, columnIndexes["updated on"]].Value);
                            var storeName = worksheet.Cells[row, columnIndexes["store name"]].Value?.ToString();
                            decimal TotalPurchasedAmount = worksheet.Cells[row, columnIndexes["total"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["total"]].Value?.ToString()) : 0;
                            var chktransactionDate = new DateTime();
                            if (transactionDate.HasValue)
                            {
                                chktransactionDate = transactionDate.Value.Date;
                            }

                            if (transactionDate != null)
                            {
                                var prooflist = new AccountingProoflistAdjustments
                                {
                                    CustomerId = customerNo,
                                    TransactionDate = transactionDate,
                                    OrderNo = orderNo,
                                    NonMembershipFee = (decimal?)0.00,
                                    PurchasedAmount = (decimal?)0.00,
                                    Amount = TotalPurchasedAmount,
                                    StatusId = 3,
                                    StoreId = await ReturnLocation(storeName),
                                    FileDescriptionId = fileIdGF,
                                    Descriptions = description,
                                    DeleteFlag = false,
                                };
                                grabFoodProofListAdj.Add(prooflist);
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
                                var prooflist = new AccountingProoflistAdjustments
                                {
                                    CustomerId = "9999011955",
                                    TransactionDate = transactionDate,
                                    OrderNo = orderNo,
                                    NonMembershipFee = (decimal?)0.00,
                                    PurchasedAmount = (decimal?)0.00,
                                    Amount = TotalPurchasedAmount,
                                    StatusId = 3,
                                    StoreId = await ReturnLocation(storeName),
                                    FileDescriptionId = fileIdGM,
                                    Descriptions = description,
                                    DeleteFlag = false,
                                };
                                grabFoodProofListAdj.Add(prooflist);
                            }
                        }
                    }
                }

                return (grabFoodProofListAdj);
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Accounting Analytics Adjustments",
                    Remarks = $"Error: Please check error in row {row}: {ex.Message}",
                    Club = strClub,
                    CustomerId = customerNo,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return (grabFoodProofListAdj);
            }
        }

        private async Task<(List<AccountingProoflist>, string?, int)> ExtractAccountingPickARoo(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo, string strClub, string userId)
        {
            var pickARooProofList = new List<AccountingProoflist>();
            var fileId = 0;
            // Define expected headers
            string[] expectedHeaders = { "order number", "outlet name", "order delivery date", "subtotal", "non membership fee amount" };
            string[] expectedHeadersFS = { "order number", "outlet name", "order delivery date", "total" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            Dictionary<string, string> customers = new Dictionary<string, string>
            {
                {  "9999011931", "Pick A Roo - Merch" },
                {  "9999011935", "Pick A Roo - FS" },
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
                            var logsDto = new LogsDto
                            {
                                UserId = userId,
                                Date = DateTime.Now,
                                Action = "Upload Accounting Analytics",
                                Remarks = $"Error: Column not found.",
                                Club = strClub,
                                CustomerId = customerNo,
                                Filename = fileName
                            };

                            var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                            _dbContext.Logs.Add(logsMap);
                            await _dbContext.SaveChangesAsync();

                            return (pickARooProofList, $"Column not found.", 0);
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
                            var storeName = worksheet.Cells[row, columnIndexes["outlet name"]].Value?.ToString();
                            decimal NonMembershipFee = worksheet.Cells[row, columnIndexes["non membership fee amount"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["non membership fee amount"]].Value?.ToString()) : 0;
                            decimal SubTotal = worksheet.Cells[row, columnIndexes["subtotal"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["subtotal"]].Value?.ToString()) : 0;
                            var storedId = await ReturnLocation(storeName);
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
                                    StoreId = storedId,
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
                            var logsDto = new LogsDto
                            {
                                UserId = userId,
                                Date = DateTime.Now,
                                Action = "Upload Accounting Analytics",
                                Remarks = $"Error: Column not found.",
                                Club = strClub,
                                CustomerId = customerNo,
                                Filename = fileName
                            };

                            var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                            _dbContext.Logs.Add(logsMap);
                            await _dbContext.SaveChangesAsync();

                            return (pickARooProofList, $"Column not found.", 0);
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
                            var storeName = worksheet.Cells[row, columnIndexes["outlet name"]].Value?.ToString();
                            decimal SubTotal = worksheet.Cells[row, columnIndexes["total"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["total"]].Value?.ToString()) : 0;
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
                                    OrderNo = worksheet.Cells[row, columnIndexes["order number"]].Value?.ToString(),
                                    NonMembershipFee = (decimal?)0.00,
                                    PurchasedAmount = (decimal?)0.00,
                                    Amount = SubTotal,
                                    StatusId = 3,
                                    StoreId = storedId,
                                    FileDescriptionId = fileId,
                                    DeleteFlag = false,
                                };
                                pickARooProofList.Add(prooflist);
                            }
                        }
                    }
                }
                return (pickARooProofList, rowCount.ToString() + " rows extracted", fileId);
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Accounting Analytics",
                    Remarks = $"Error: Please check error in row {row}: {ex.Message}",
                    Club = strClub,
                    CustomerId = customerNo,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return (pickARooProofList, "Error extracting proof list.", 0);
            }
        }
       
        private async Task<List<AccountingProoflistAdjustments>> ExtractAccountingPickARooAdjustments(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo, string strClub, string userId, int? fileId)
        {
            var pickARooProofListAdj = new List<AccountingProoflistAdjustments>();
            string[] expectedHeaders = { "date", "order number", "adjustment to sales" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            Dictionary<string, string> customers = new Dictionary<string, string>
            {
                {  "9999011931", "Pick A Roo - Merch" },
                {  "9999011935", "Pick A Roo - FS" },
            };

            customers.TryGetValue(customerNo, out string valueCust);

            try
            {
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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Accounting Analytics Adjustments",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            CustomerId = customerNo,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        await _dbContext.SaveChangesAsync();

                        return pickARooProofListAdj;
                    }
                }

                for (row = 2; row <= rowCount; row++)
                {
                    var transactionDate = new DateTime?();
                    if (worksheet.Cells[row, columnIndexes["date"]].Value != null &&
                        worksheet.Cells[row, columnIndexes["order number"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["adjustment to sales"]].Value != null)
                    {
                        if (worksheet.Cells[row, columnIndexes["date"]].Value != null)
                        {
                            if (worksheet.Cells[row, columnIndexes["date"]].Value.ToString().Contains("-"))
                            {
                                string pattern = @"-\d+";
                                string cleanedInput = Regex.Replace(worksheet.Cells[row, columnIndexes["date"]].Value.ToString(), pattern, "");
                                DateTime date = DateTime.ParseExact(cleanedInput, "MMMM d, yyyy", CultureInfo.InvariantCulture);
                                string formattedDate = date.ToString("M/d/yyyy h:mm:ss tt");
                                transactionDate = GetDateTimeFoodPandaAdj(formattedDate);

                            }
                            else
                            {
                                if (IsValidFormat(worksheet.Cells[row, columnIndexes["date"]].Value.ToString(), "MM/d/yyyy"))
                                {
                                    DateTime date = DateTime.ParseExact(worksheet.Cells[row, columnIndexes["date"]].Value.ToString(), "MM/d/yyyy", CultureInfo.InvariantCulture);
                                    string formattedDate = date.ToString("M/d/yyyy h:mm:ss tt");
                                    transactionDate = GetDateTimeFoodPandaAdj(formattedDate);
                                }
                                else
                                {
                                    transactionDate = GetDateTimeFoodPandaAdj(worksheet.Cells[row, columnIndexes["date"]].Value);
                                }
                            }
                            var orderNo = worksheet.Cells[row, columnIndexes["order number"]].Value?.ToString();
                            decimal netTotal = worksheet.Cells[row, columnIndexes["adjustment to sales"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["adjustment to sales"]].Value?.ToString()) : 0;

                            if (transactionDate != null)
                            {
                                var prooflistAdj = new AccountingProoflistAdjustments
                                {
                                    CustomerId = customerNo,
                                    TransactionDate = transactionDate,
                                    OrderNo = orderNo,
                                    NonMembershipFee = (decimal?)0.00,
                                    PurchasedAmount = (decimal?)0.00,
                                    Amount = netTotal,
                                    StatusId = 3,
                                    StoreId = 0,
                                    FileDescriptionId = fileId,
                                    DeleteFlag = false,

                                };
                                pickARooProofListAdj.Add(prooflistAdj);
                            }
                        }
                    }
                }

                return (pickARooProofListAdj);
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Accounting Analytics Adjustments",
                    Remarks = $"Error: Please check error in row {row}: {ex.Message}",
                    Club = strClub,
                    CustomerId = customerNo,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return (pickARooProofListAdj);
            }
        }
        
        private async Task<(List<AccountingProoflist>, string?, int?)> ExtractAccountingFoodPanda(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo, string strClub, string userId)
        {
            var foodPandaProofList = new List<AccountingProoflist>();
            var fileId = 0;
            // Define expected headers
            string[] expectedHeaders = { "order code", "order date", "gross food value / product value", "vendor code" };

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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Accounting Analytics",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            CustomerId = customerNo,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        await _dbContext.SaveChangesAsync();

                        return (foodPandaProofList, $"Column not found.", fileId);
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
                    if (worksheet.Cells[row, columnIndexes["order code"]].Value != null &&
                        worksheet.Cells[row, columnIndexes["order date"]].Value != null &&
                        worksheet.Cells[row, columnIndexes["gross food value / product value"]].Value != null &&
                        worksheet.Cells[row, columnIndexes["vendor code"]].Value != null)
                    {
                        var transactionDate = GetDateTimeFoodPanda(worksheet.Cells[row, columnIndexes["order date"]].Value);
                        var vendorCode = worksheet.Cells[row, columnIndexes["vendor code"]].Value?.ToString();
                        decimal TotalPurchasedAmount = worksheet.Cells[row, columnIndexes["gross food value / product value"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["gross food value / product value"]].Value?.ToString()) : 0;
                        var chktransactionDate = new DateTime();
                        var storedId = await ReturnLocationByVendor(vendorCode);
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
                                StoreId = storedId,
                                FileDescriptionId = fileId,
                                DeleteFlag = false,
                            };
                            foodPandaProofList.Add(prooflist);
                        }
                    }
                }

                return (foodPandaProofList, rowCount.ToString() + " rows extracted", fileId);
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Accounting Analytics",
                    Remarks = $"Error: Please check error in row {row}: {ex.Message}",
                    Club = strClub,
                    CustomerId = customerNo,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return (foodPandaProofList, "Error extracting proof list.", fileId);
            }
        }
        
        private async Task<List<AccountingProoflistAdjustments>> ExtractAccountingFoodPandaAdjustments(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo, string strClub, string userId, int? fileId)
        {
            var foodPandaProofListAdj = new List<AccountingProoflistAdjustments>();
            string[] expectedHeaders = { "invoice date", "vendor code", "category", "description", "net total" };

            Dictionary<string, int> columnIndexes = new Dictionary<string, int>();
            Dictionary<string, string> customers = new Dictionary<string, string>
            {
                {  "9999011838", "Food Panda" },
            };

            customers.TryGetValue(customerNo, out string valueCust);

            try
            {
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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Accounting Analytics Adjustments",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            CustomerId = customerNo,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        await _dbContext.SaveChangesAsync();

                        return foodPandaProofListAdj;
                    }
                }

                for (row = 2; row <= rowCount; row++)
                {
                   
                    if (worksheet.Cells[row, columnIndexes["invoice date"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["vendor code"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["category"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["description"]].Value != null ||
                        worksheet.Cells[row, columnIndexes["net total"]].Value != null)
                    {
                        var transactionDate = GetDateTimeFoodPandaAdj(worksheet.Cells[row, columnIndexes["invoice date"]].Value);
                        var vendorCode = worksheet.Cells[row, columnIndexes["vendor code"]].Value?.ToString();
                        decimal netTotal = worksheet.Cells[row, columnIndexes["net total"]].Value != null ? decimal.Parse(worksheet.Cells[row, columnIndexes["net total"]].Value?.ToString()) : 0;
                        var storedId = await ReturnLocationByVendor(vendorCode);
                        var category = worksheet.Cells[row, columnIndexes["category"]].Value?.ToString();
                        var description = worksheet.Cells[row, columnIndexes["description"]].Value?.ToString().Trim();
                        var orderNo = "";

                        if (category != "Platform Fee")
                        {
                            if (category == "Comm. rev. - OD dr")
                            {
                                orderNo = ExtractOrderNumberFromCommRev(description);
                            }
                            else if (category == "Wastage")
                            {
                                orderNo = ExtractOrderNumberFromWastage(description);
                            }
                            else if (category == "Penalty")
                            {
                                orderNo = ExtractOrderNumberFromPenalty(description);
                            }
                            else
                            {
                                orderNo = ExtractOrderNumberFromOtherCategories(description);
                            }
                        }

                        if (transactionDate != null)
                        {
                            var prooflistAdj = new AccountingProoflistAdjustments
                            {
                                CustomerId = customerNo,
                                TransactionDate = transactionDate,
                                OrderNo = orderNo,
                                NonMembershipFee = (decimal?)0.00,
                                PurchasedAmount = (decimal?)0.00,
                                Amount = netTotal,
                                StatusId = 3,
                                StoreId = storedId,
                                FileDescriptionId = fileId,
                                Category = category,
                                Descriptions = description,
                                DeleteFlag = false,

                            };
                            foodPandaProofListAdj.Add(prooflistAdj);
                        }
                    }
                }

                return (foodPandaProofListAdj);
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Accounting Analytics Adjustments",
                    Remarks = $"Error: Please check error in row {row}: {ex.Message}",
                    Club = strClub,
                    CustomerId = customerNo,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return (foodPandaProofListAdj);
            }
        }
        
        private async Task<(List<AccountingProoflist>, string?)> ExtractAccountingMetroMart(ExcelWorksheet worksheet, int rowCount, int row, string fileName, string customerNo, string strClub, string userId)
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
                        var logsDto = new LogsDto
                        {
                            UserId = userId,
                            Date = DateTime.Now,
                            Action = "Upload Accounting Analytics",
                            Remarks = $"Error: Column not found.",
                            Club = strClub,
                            CustomerId = customerNo,
                            Filename = fileName
                        };

                        var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        await _dbContext.SaveChangesAsync();

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
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Upload Accounting Analytics",
                    Remarks = $"Error: Please check error in row {row}: {ex.Message}",
                    Club = strClub,
                    CustomerId = customerNo,
                    Filename = fileName
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return (metroMartProofList, "Error extracting proof list.");
            }
        }

        private async Task<int?> ReturnLocation(string location)
        {
            if (location != null || location != string.Empty)
            {
                location = location.Trim();
                string formatLocation = location.Replace("S&R New York Style Pizza - ", "");
                string removeText = formatLocation.Replace("[Available for LONG-DISTANCE DELIVERY]", "");
                string removeText1 = removeText.Replace("Libis Warehouse", "");
                string removeText2 = removeText1.Replace("Ilo-ilo Warehouse", "ILOILO");
                string removeText3 = removeText2.Replace("Evo City", "");
                string locationName = removeText3.Replace("S&R - ", "");
                string formatLocName = locationName.Replace("BGC", "FORT");
                string formatLocName1 = formatLocName.Replace("Libis", "C5");
                string formatLocName2 = formatLocName1.Replace("Bonifacio Global City", "FORT");
                string formatLocName3= formatLocName2.Replace("S&R Membership - ", "");
                string formatLocName4 = formatLocName3.Replace("ParaÃ±aque", "Paranaque");
                string formatLocName5 = formatLocName4.Replace("C5 Libis", "C5");
                string formatLocName6 = formatLocName5.Replace("C5 C5", "C5");

                formatLocName6 = formatLocName6.Trim();

                var formatLocations = await _dbContext.Locations.Where(x => x.LocationName.ToLower().Contains("kareila"))
                    .ToListAsync();

                var getLocationCode =  formatLocations.Where(x => x.LocationName.ToLower().Contains(formatLocName6.ToLower()))
                .Select(n => n.LocationCode)
                .FirstOrDefault();

                return getLocationCode;
            }
            else
            {
                return null;
            }
        }

        private async Task<int?> ReturnLocationByVendor(string vendorCode)
        {
            if (vendorCode != null || vendorCode != string.Empty)
            {
                var getLocationCode = await _dbContext.Locations.Where(x => x.VendorCode.ToLower() == vendorCode.ToLower())
                .Select(n => n.LocationCode)
                .FirstOrDefaultAsync(); ;

                return getLocationCode;
            }
            else
            {
                return null;
            }
        }

        public async Task<bool> DeleteAccountingAnalytics(UpdateAnalyticsDto updateAnalyticsDto)
        {
            var result = false;
            var customerId = "";
            try
            {
                var fileDescDelete = await _dbContext.FileDescription
               .Where(x => x.Id == updateAnalyticsDto.Id)
               .ToListAsync();

                if (fileDescDelete != null)
                {
                    _dbContext.FileDescription.RemoveRange(fileDescDelete);
                    await _dbContext.SaveChangesAsync();
                }

                var accountingProoflistsDelete = await _dbContext.AccountingProoflists
                    .Where(x => x.FileDescriptionId == updateAnalyticsDto.Id)
                    .ToListAsync();

                customerId = accountingProoflistsDelete.Select(x => x.CustomerId).FirstOrDefault();

                if (accountingProoflistsDelete != null)
                {
                    _dbContext.AccountingProoflists.RemoveRange(accountingProoflistsDelete);
                    await _dbContext.SaveChangesAsync();

                    var logsDto = new LogsDto
                    {
                        UserId = updateAnalyticsDto.UserId,
                        Date = DateTime.Now,
                        Action = "Delete Accounting Analytics",
                        Remarks = $"Successfully Deleted",
                        Club = updateAnalyticsDto.StoreId,
                        CustomerId = customerId,
                    };

                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();
                }
                return true;
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = updateAnalyticsDto.UserId,
                    Date = DateTime.Now,
                    Action = "Delete Accounting Analytics",
                    Remarks = $"Error: {ex.Message}",
                    Club = updateAnalyticsDto.StoreId,
                    CustomerId = customerId,
                };

                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

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
                    .GroupJoin(
                        _dbContext.Locations,
                        a => a.StoreId,
                        b => b.LocationCode,
                        (a, locationGroup) => new { a, locationGroup })
                    .SelectMany(
                        x => x.locationGroup.DefaultIfEmpty(),
                        (x, b) => new { x.a, b })
                    .Join(
                        _dbContext.Status,
                        c => c.a.StatusId,
                        d => d.Id,
                        (c, d) => new { c, d })
                    .Where(x => x.c.a.TransactionDate.Value.Date >= date1.Value.Date
                                && x.c.a.TransactionDate.Value.Date <= date2.Value.Date
                                && x.c.a.CustomerId == portalParamsDto.memCode[0]
                                && x.c.a.StatusId != 4
                                && x.c.a.OrderNo.Contains(portalParamsDto.orderNo))
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
                        StoreName = n.c.b != null ? n.c.b.LocationName : null,
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

        public DateTime? GetDateTimeFoodPandaAdj(object cellValue)
        {
            if (cellValue != null)
            {
                // Specify the format of the date string
                if (DateTime.TryParseExact(cellValue.ToString(), "M/dd/yyyy h:mm:ss tt", CultureInfo.InvariantCulture, DateTimeStyles.None, out var transactionDate))
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

        private string ExtractOrderNumberFromCommRev(string description)
        {
            string searchPhrase = "Commission fee for ";
            int startIndex = description.IndexOf(searchPhrase);

            if (startIndex != -1)
            {
                startIndex += searchPhrase.Length;

                int endIndex = description.IndexOf(' ', startIndex);
                if (endIndex == -1)
                {
                    endIndex = description.Length;
                }

                string orderNo = description.Substring(startIndex, endIndex - startIndex).Trim();

                orderNo = orderNo.TrimEnd(':', ',', ' ');

                return orderNo;
            }

            return "";
        }

        private string ExtractOrderNumberFromWastage(string description)
        {
            string searchPhrase = "Missing Items payment: ";
            int startIndex = description.IndexOf(searchPhrase);

            if (startIndex != -1)
            {
                startIndex += searchPhrase.Length;

                int endIndex = description.IndexOf(' ', startIndex);
                int commaIndex = description.IndexOf(',', startIndex);

                if (endIndex == -1 || (commaIndex != -1 && commaIndex < endIndex))
                {
                    endIndex = commaIndex;
                }

                string orderNo = description.Substring(startIndex, endIndex - startIndex).Trim();

                orderNo = orderNo.TrimEnd(':', ',', ' ');

                return orderNo;
            }

            return "";
        }
       
        private string ExtractOrderNumberFromPenalty(string description)
        {
            int colonIndex = description.IndexOf(':');

            if (colonIndex != -1)
            {
                string orderNo = description.Substring(colonIndex + 1).Trim();

                orderNo = orderNo.TrimEnd(':', ',', ' ');

                return orderNo;
            }

            return "";
        }

        private string ExtractOrderNumberFromOtherCategories(string description)
        {
            return "";
        }

        static bool IsValidFormat(string input, string format)
        {
            DateTime tempDate;
            return DateTime.TryParseExact(input, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out tempDate);
        }
    }
}
