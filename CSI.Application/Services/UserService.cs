using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using System.DirectoryServices.AccountManagement;
using System.DirectoryServices;
using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Authorization;
using AutoMapper;

namespace CSI.Application.Services
{
    public class UserService : IUserService
    {
        private readonly AppDBContext _dbContext;
        private readonly IPasswordHashService _passwordHashService;
        private readonly IJwtService _jwtService;
        private readonly IMapper _mapper;
        private readonly int saltiness = 70;
        private readonly int nIterations = 10101;

        public UserService(AppDBContext dbContext, IPasswordHashService passwordHashService, IJwtService jwtService, IMapper mapper)
        {
            _dbContext = dbContext;
            _passwordHashService = passwordHashService;
            _jwtService = jwtService;
            _mapper = mapper;
        }

        public async Task<UserDto> AuthenticateAsync(string username, string password)
        {
            int salt = Convert.ToInt32(saltiness);
            int iterations = Convert.ToInt32(nIterations);
            string Token = "";
            var logsDto = new LogsDto();
            var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
            var strClub = string.Empty;

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
                    strClub = result.Club.ToString();
                    if (!result.IsLogin)
                    {
                        var HashedPassword = _passwordHashService.HashPassword(password, result.Salt, iterations, salt);

                        if (result.Hash == HashedPassword)
                        {
                            result.IsLogin = false;
                            _ = await _dbContext.SaveChangesAsync();

                            Token = _jwtService.GenerateToken(result);

                            logsDto = new LogsDto
                            {
                                UserId = result.Id.ToString(),
                                Date = DateTime.Now,
                                Action = "Login",
                                Remarks = $"Login Successful",
                                Club = strClub,
                            };

                            logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                            _dbContext.Logs.Add(logsMap);
                            await _dbContext.SaveChangesAsync();

                            return new UserDto
                            {
                                Id = result.Id,
                                EmployeeNumber = result.EmployeeNumber,
                                FirstName = result.FirstName,
                                LastName = result.LastName,
                                Username = result.Username,
                                IsLogin = true,
                                RoleId = result.RoleId,
                                Club = result.Club,
                                Token = Token,
                                Message = "Login Successful"
                            };
                        }

                        return new UserDto();
                    }

                    logsDto = new LogsDto
                    {
                        UserId = result.Id.ToString(),
                        Date = DateTime.Now,
                        Action = "Login",
                        Remarks = $"User is already logged in.",
                        Club = strClub,
                    };

                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

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
            var logsDto = new LogsDto();
            var strClub = string.Empty;
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
                    strClub = result.Club.ToString();
                    result.IsLogin = false;
                    _ = await _dbContext.SaveChangesAsync();

                    logsDto = new LogsDto
                    {
                        UserId = result.Id.ToString(),
                        Date = DateTime.Now,
                        Action = "Logout",
                        Remarks = $"Logout Successful",
                        Club = strClub,
                    };

                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

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
    }
}
