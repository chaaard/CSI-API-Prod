using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using System.DirectoryServices.AccountManagement;
using System.DirectoryServices;
using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Authorization;

namespace CSI.Application.Services
{
    public class UserService : IUserService
    {
        private readonly AppDBContext _dbContext;
        private readonly IPasswordHashService _passwordHashService;
        private readonly IJwtService _jwtService;
        private readonly int saltiness = 70;
        private readonly int nIterations = 10101;

        public UserService(AppDBContext dbContext, IPasswordHashService passwordHashService, IJwtService jwtService)
        {
            _dbContext = dbContext;
            _passwordHashService = passwordHashService;
            _jwtService = jwtService;
        }

        public async Task<UserDto> AuthenticateAsync(string username, string password)
        {
            int salt = Convert.ToInt32(saltiness);
            int iterations = Convert.ToInt32(nIterations);
            string Token = "";

            if (username != null && password != null)
            {
                var result = await _dbContext.Users
                    .Where(u => u.Username == username)
                    .FirstOrDefaultAsync();

                if (result == null)
                {
                    return new UserDto();
                }
                else
                {
                    if (!result.IsLogin)
                    {
                        var HashedPassword = _passwordHashService.HashPassword(password, result.Salt, iterations, salt);

                        if (result.Hash == HashedPassword)
                        {
                            result.IsLogin = true;
                            _ = await _dbContext.SaveChangesAsync();

                            Token = _jwtService.GenerateToken(result);
                            return new UserDto
                            {
                                Id = result.Id,
                                EmployeeNumber = result.EmployeeNumber,
                                FirstName = result.FirstName,
                                LastName = result.LastName,
                                Username = result.Username,
                                IsLogin = false,
                                RoleId = result.RoleId,
                                Club = result.Club,
                                Token = Token,
                                Message = "Login Successful"
                            };
                        }

                        return new UserDto();
                    }

                    return new UserDto
                    {
                        Id = result.Id,
                        EmployeeNumber = result.EmployeeNumber,
                        FirstName = result.FirstName,
                        LastName = result.LastName,
                        Username = result.Username,
                        IsLogin = result.IsLogin,
                        RoleId = result.RoleId,
                        Club = result.Club,
                        Token = Token,
                        Message = "User is already logged in."
                    };
                }
            }
            else
            {
                return new UserDto();
            }
        }

        [SuppressMessage("Interoperability", "CA1416:Validate platform compatibility", Justification = "<Pending>")]
        public async Task<UserDto> AuthenticateADAsync()
        {
            string Token = "";
            string domainName = "snrshopping";

            using (var context = new PrincipalContext(ContextType.Domain, domainName))
            {
                string username = Environment.UserName;

                using (var searcher = new DirectorySearcher())
                {
                    searcher.Filter = $"(&(objectClass=user)(sAMAccountName={username}))";

                    SearchResult result = searcher.FindOne()!;

                    if (result != null)
                    {
                        var userInDb = await _dbContext.Users
                            .Where(u => u.Username == username)
                            .FirstOrDefaultAsync();

                        if (userInDb == null)
                        {
                            return new UserDto();
                        }
                        else
                        {
                            if (!userInDb.IsLogin)
                            {
                                userInDb.IsLogin = true;
                                _ = await _dbContext.SaveChangesAsync();

                                Token = _jwtService.GenerateToken(userInDb);
                                return new UserDto
                                {
                                    Id = userInDb.Id,
                                    EmployeeNumber = userInDb.EmployeeNumber,
                                    FirstName = userInDb.FirstName,
                                    LastName = userInDb.LastName,
                                    Username = userInDb.Username,
                                    IsLogin = userInDb.IsLogin,
                                    RoleId = userInDb.RoleId,
                                    Club = userInDb.Club,
                                    Token = Token,
                                    Message = "Login Successful"
                                };
                            }
                            else
                            {
                                return new UserDto
                                {
                                    Id = userInDb.Id,
                                    EmployeeNumber = userInDb.EmployeeNumber,
                                    FirstName = userInDb.FirstName,
                                    LastName = userInDb.LastName,
                                    Username = userInDb.Username,
                                    IsLogin = userInDb.IsLogin,
                                    RoleId = userInDb.RoleId,
                                    Club = userInDb.Club,
                                    Token = Token,
                                    Message = "User is already logged in."
                                };
                            }
                        }
                    }
                }

                return new UserDto();
            }
        }

        public async Task<UserDto> Logout(string username)
        {
            if (username != null)
            {
                var result = await _dbContext.Users
                    .Where(u => u.Username == username)
                    .FirstOrDefaultAsync();

                if (result == null)
                {
                    return new UserDto();
                }
                else
                {
                   
                    result.IsLogin = false;
                    _ = await _dbContext.SaveChangesAsync();

                    return new UserDto
                    {
                        Id = result.Id,
                        EmployeeNumber = result.EmployeeNumber,
                        FirstName = result.FirstName,
                        LastName = result.LastName,
                        Username = result.Username,
                        IsLogin = result.IsLogin,
                        RoleId = result.RoleId,
                        Club = result.Club,
                        Token = "",
                        Message = "Logout Successful"
                    };
                   
                    return new UserDto();
                }
            }
            else
            {
                return new UserDto();
            }
        }

        public async Task<bool?> IsLogin(string username)
        {
            var isLogin = false; //Login = true
            if (username != null)
            {
                var result = await _dbContext.Users
                     .Where(u => u.Username == username)
                     .FirstOrDefaultAsync();
                if (result != null)
                {
                    isLogin = result.IsLogin;
                    return isLogin;
                }
                return null;
            }
            else
            {
                return null;
            }
        }

        public async Task<UserInfoDto> GetUserInfo(string username)
        {
            var result = new UserInfoDto();
            if (username != null)
            {
                
                result = await _dbContext.Users
                    .Where(u => u.Username == username)
                    .Join(_dbContext.Roles, a => a.RoleId, b => b.Id, (a, b) => new { a,b })
                    .Join(_dbContext.Locations, c => c.a.Club, d => d.LocationCode, (c, d) => new { c,d })
                    .Select(n => new UserInfoDto 
                    {
                        Role = n.c.b.Role,
                        Club = n.d.LocationName,
                    })
                    .FirstOrDefaultAsync();

                return result;
            }
            else
            {
                return result;
            }
        }

        //[Authorize]
        //public async Task<(List<UserListDto>, int)> GetAllUsers()
        //{
        //    try
        //    {
        //        //var userList = new List<UserListDto>();
        //        var query = _dbContext.Users
        //            .Where(x => x.RoleId != 4)
        //            .Join(_dbContext.Roles, x => x.RoleId, y => y.Id, (x, y) => new { x, y })
        //            .Join(_dbContext.Locations, user => user.x.Club, loc => loc.LocationCode, (user, loc) => new { user, loc })
        //            .Select(n => new UserListDto 
        //            {
        //                Id = n.user.x.Id,
        //                EmployeeNumber = n.user.x.EmployeeNumber,
        //                FirstName = n.user.x.FirstName,
        //                LastName = n.user.x.LastName,
        //                Username = n.user.x.Username,
        //                Role = n.user.y.Role,
        //                Club = n.loc.LocationName,
        //                IsLogin = n.user.x.IsLogin,
        //            })
        //            .AsQueryable();
        //        return userList;

        //        var query = _dbContext.CustomerCodes
        //       .Where(customerCode => customerCode.DeleteFlag == false)
        //       .Select(n => new CustomerCodeDto
        //       {
        //           Id = n.Id,
        //           CustomerNo = n.CustomerNo,
        //           CustomerCode = n.CustomerCode,
        //           CustomerName = n.CustomerName,
        //           DeleteFlag = n.DeleteFlag,
        //       })
        //       .AsQueryable();

        //        // Searching
        //        if (!string.IsNullOrEmpty(pagination.SearchQuery))
        //        {
        //            var searchQuery = $"%{pagination.SearchQuery.ToLower()}%";

        //            query = query.Where(c =>
        //                (EF.Functions.Like(c.CustomerName.ToLower(), searchQuery)) ||
        //                (EF.Functions.Like(c.CustomerCode.ToLower(), searchQuery))
        //            //Add the category column here
        //            );
        //        }

        //        // Sorting
        //        if (!string.IsNullOrEmpty(pagination.ColumnToSort))
        //        {
        //            var sortOrder = pagination.OrderBy == "desc" ? "desc" : "asc";

        //            switch (pagination.ColumnToSort.ToLower())
        //            {
        //                case "customername":
        //                    query = sortOrder == "asc" ? query.OrderBy(c => c.CustomerName) : query.OrderByDescending(c => c.CustomerName);
        //                    break;
        //                case "customercode":
        //                    query = sortOrder == "asc" ? query.OrderBy(c => c.CustomerCode) : query.OrderByDescending(c => c.CustomerCode);
        //                    break;
        //                //Another case here for category
        //                default:
        //                    break;
        //            }
        //        }

        //        var totalItemCount = await query.CountAsync();
        //        var totalPages = (int)Math.Ceiling((double)totalItemCount / pagination.PageSize);

        //        var customerCodesList = await query
        //            .Skip((pagination.PageNumber - 1) * pagination.PageSize)
        //            .Take(pagination.PageSize)
        //            .ToListAsync();

        //        return (customerCodesList, totalPages);
        //    }
        //    catch (Exception)
        //    {

        //        throw;
        //    }
        //}
    }
}
