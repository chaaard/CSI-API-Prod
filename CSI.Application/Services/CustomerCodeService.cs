
using AutoMapper;
using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using System;

namespace CSI.Application.Services
{
    public class CustomerCodeService : ICustomerCodeService
    {
        private readonly AppDBContext _dbContext;
        private readonly IConfiguration _configuration;
        private readonly IMapper _mapper;

        public CustomerCodeService(IConfiguration configuration, AppDBContext dBContext, IMapper mapper)
        {
            _configuration = configuration;
            _dbContext = dBContext;
            _mapper = mapper;
        }

        public async Task<(List<CustomerCodeDto>, int totalPages)> GetCustomerCodesAsync(PaginationDto pagination)
        {
            var query = _dbContext.CustomerCodes
                .Select(n => new CustomerCodeDto { 
                    Id = n.Id,
                    CustomerNo = n.CustomerNo,
                    CustomerCode = n.CustomerCode,
                    CustomerName = n.CustomerName,
                    DeleteFlag = n.DeleteFlag,
                })
                .AsQueryable();

            // Searching
            if (!string.IsNullOrEmpty(pagination.SearchQuery))
            {
                var searchQuery = $"%{pagination.SearchQuery.ToLower()}%";

                query = query.Where(c =>
                    (EF.Functions.Like(c.CustomerNo.ToLower(), searchQuery)) ||
                    (EF.Functions.Like(c.CustomerName.ToLower(), searchQuery)) ||
                    (EF.Functions.Like(c.CustomerCode.ToLower(), searchQuery))
                //Add the category column here
                );
            }

            // Sorting
            if (!string.IsNullOrEmpty(pagination.ColumnToSort))
            {
                var sortOrder = pagination.OrderBy == "desc" ? "desc" : "asc";

                switch (pagination.ColumnToSort.ToLower())
                {
                    case "customername":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.CustomerName) : query.OrderByDescending(c => c.CustomerName);
                        break;
                    case "customercode":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.CustomerCode) : query.OrderByDescending(c => c.CustomerCode);
                        break;
                    case "customerno":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.CustomerNo) : query.OrderByDescending(c => c.CustomerNo);
                        break;
                    case "deleteflag":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.DeleteFlag) : query.OrderByDescending(c => c.DeleteFlag);
                        break;
                    //Another case here for category
                    default:
                        break;
                }
            }

            var totalItemCount = await query.CountAsync();
            var totalPages = (int)Math.Ceiling((double)totalItemCount / pagination.PageSize);

            var customerCodesList = await query
                .Skip((pagination.PageNumber - 1) * pagination.PageSize)
                .Take(pagination.PageSize)
                .ToListAsync();

            return (customerCodesList, totalPages);
        }

        public async Task<List<CustomerCodes>> GetCustomerDdCodesAsync()
        {
            var query = await _dbContext.CustomerCodes
                .ToListAsync();

            return query;
        }

        public async Task<CustomerCodes> GetCustomerCodeByIdAsync(int Id)
        {
            var getCustomerCodes = new CustomerCodes();
            getCustomerCodes = await _dbContext.CustomerCodes.Where(x => x.Id == Id).FirstAsync();
            return getCustomerCodes;
        }

        public async Task<CustomerCodes> UpdateCustomerCodeByIdAsync(CustomerCodeDto customerCode)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var getCustomerCode = await _dbContext.CustomerCodes.SingleOrDefaultAsync(x => x.Id == customerCode.Id);

                if (getCustomerCode != null)
                {
                    var oldCustomerName = getCustomerCode.CustomerName;
                    var oldCustomerCode = getCustomerCode.CustomerCode;
                    var oldCustomerNo = getCustomerCode.CustomerNo;
                    var oldDeleteFlag = getCustomerCode.DeleteFlag;
                    getCustomerCode.CustomerName = customerCode.CustomerName;
                    getCustomerCode.CustomerCode = customerCode.CustomerCode;
                    getCustomerCode.CustomerNo = customerCode.CustomerNo;
                    getCustomerCode.DeleteFlag = customerCode.DeleteFlag;
                    await _dbContext.SaveChangesAsync();

                    logsDto = new LogsDto
                    {
                        UserId = customerCode.UserId,
                        Date = DateTime.Now,
                        Action = "Update Merchant",
                        Remarks = $"Id: {customerCode.Id} : " +
                                  $"CustomerName: {oldCustomerName} -> {customerCode.CustomerName}, " +
                                  $"CustomerCode: {oldCustomerCode} -> {customerCode.CustomerCode}, " +
                                  $"CustomerNo: {oldCustomerNo} -> {customerCode.CustomerNo}, " +
                                  $"DeleteFlag: {oldDeleteFlag} -> {customerCode.DeleteFlag}"
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

                    return getCustomerCode;
                }
                else
                {
                    return new CustomerCodes();
                }
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = customerCode.UserId,
                    Date = DateTime.Now,
                    Action = "Update Merchant",
                    Remarks = $"Error: {ex.Message}",
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<CustomerCodes> InsertCustomerCodeAsync(CustomerCodes customerCode)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var getCustomerCode = await _dbContext.CustomerCodes
                    .SingleOrDefaultAsync(x => x.CustomerCode == customerCode.CustomerCode 
                                            || x.CustomerNo == customerCode.CustomerNo
                                            || x.CustomerName == customerCode.CustomerName);

                if (getCustomerCode == null)
                {
                    // Define a lambda expression for inserting the user
                    Func<CustomerCodes, Task<CustomerCodes>> insertCustomerLambda = async newUser =>
                    {
                        var getCustomer = new CustomerCodes
                        {
                            CustomerNo = newUser.CustomerNo,
                            CustomerName = newUser.CustomerName,
                            CustomerCode = newUser.CustomerCode,
                            DeleteFlag = newUser.DeleteFlag,
                        };

                        await _dbContext.CustomerCodes.AddAsync(getCustomer);
                        await _dbContext.SaveChangesAsync(); // Ensure changes are saved to the database
                        return getCustomer;
                    };

                    // Invoke the lambda to insert the customer
                    return await insertCustomerLambda(customerCode);
                }
                return null;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    Date = DateTime.Now,
                    Action = "Insert Merchant",
                    Remarks = $"Error: {ex.Message}",
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<bool> DeleteCustomerCodeByIdAsync(int Id)
        {
            var getCustomerCode = await _dbContext.CustomerCodes.SingleOrDefaultAsync(x => x.Id == Id);

            if (getCustomerCode != null)
            {
                getCustomerCode.DeleteFlag = true;
                await _dbContext.SaveChangesAsync();

                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
