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
                    if (!result.IsLogin)
                    {
                        var HashedPassword = _passwordHashService.HashPassword(password, result.Salt, iterations, salt);

                        if (result.Hash == HashedPassword)
                        {
                            result.Attempt = 0;
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
                                IsFirstLogin = result.IsFirstLogin,
                                RoleId = result.RoleId,
                                Club = result.Club,
                                Attempt = result.Attempt,
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
                        IsFirstLogin = result.IsFirstLogin,
                        RoleId = result.RoleId,
                        Club = result.Club,
                        Attempt = 0,
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
                                    IsFirstLogin = userInDb.IsFirstLogin,
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
                                    IsFirstLogin = userInDb.IsFirstLogin,
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
                        IsFirstLogin = result.IsFirstLogin,
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

        public async Task<UserDto> LoginAttempt(string username)
        {
            var getUser = await _dbContext.Users.SingleOrDefaultAsync(x => x.Username == username);

            if (getUser != null)
            {
                if (getUser.Attempt >= 5)
                {
                    return new UserDto
                    {
                        EmployeeNumber = getUser.EmployeeNumber,
                        Username = getUser.Username,
                        Attempt = getUser.Attempt,
                        Token = "",
                        Message = "Login attempt limit reached!"
                    };
                }
                else
                {
                    getUser.Attempt = getUser.Attempt + 1;
                    await _dbContext.SaveChangesAsync();
                    return new UserDto
                    {
                        EmployeeNumber = getUser.EmployeeNumber,
                        Username = getUser.Username,
                        Attempt = getUser.Attempt,
                        Token = "",
                        Message = "Successful"
                    };
                }
            }
            else
            {
                return new UserDto();
            }
        }

        public async Task<UserDto> ChangePassword(string username, string password)
        {
            int salt = Convert.ToInt32(saltiness);
            int iterations = Convert.ToInt32(nIterations);
            if (username != null && password != null)
            {
                var result = await _dbContext.Users
                     .Where(u => u.Username == username)
                     .FirstOrDefaultAsync();
                if (result != null)
                {
                    var HashedPassword = _passwordHashService.HashPassword(password, result.Salt, iterations, salt);
                    result.Hash = HashedPassword;                    
                    result.IsFirstLogin = false;
                    await _dbContext.SaveChangesAsync();
                }
                return new UserDto
                {
                    Id = result.Id,
                    EmployeeNumber = result.EmployeeNumber,
                    FirstName = result.FirstName,
                    LastName = result.LastName,
                    Username = result.Username,
                    IsLogin = result.IsLogin,
                    IsFirstLogin = result.IsFirstLogin,
                    RoleId = result.RoleId,
                    Club = result.Club,
                    Token = "",
                    Message = "Successful"
                };
            }
            else
            {
                return new UserDto();
            }
        }
        
        public async Task<(List<UserDto>, int totalPages)> GetUsersListAsync(PaginationDto pagination)
        {
            var query = _dbContext.Users
                .Join(_dbContext.Roles, x => x.RoleId, y => y.Id, (x, y) => new { x, y })
                .Join(_dbContext.Locations, xy => xy.x.Club, z => z.LocationCode, (xy, z) => new { xy.x, xy.y, z })
                .Select(n => new UserDto
                {
                    Id = n.x.Id,
                    Username = n.x.Username,
                    EmployeeNumber = n.x.EmployeeNumber,
                    FirstName = n.x.FirstName,
                    MiddleName = n.x.MiddleName,
                    LastName = n.x.LastName,
                    Salt = n.x.Salt,
                    Hash = n.x.Hash,
                    Club = n.x.Club,
                    Location = n.z.LocationName.Replace("KAREILA", n.x.Club.ToString()).Trim(),
                    RoleId = n.x.RoleId,
                    RoleName = n.y.Role,
                    IsLogin = n.x.IsLogin,
                    IsFirstLogin = n.x.IsFirstLogin,
                    Status = n.x.Status,
                    Attempt = n.x.Attempt
                })
                .AsQueryable();

            // Searching
            if (!string.IsNullOrEmpty(pagination.SearchQuery))
            {
                var searchQuery = $"%{pagination.SearchQuery.ToLower()}%";

                query = query.Where(c =>
                    //Add the category column here
                    (EF.Functions.Like(c.Username.ToLower(), searchQuery)) ||
                    (EF.Functions.Like(c.FirstName.ToLower(), searchQuery)) ||
                    (EF.Functions.Like(c.LastName.ToLower(), searchQuery))
                );
            }

            // Sorting
            if (!string.IsNullOrEmpty(pagination.ColumnToSort))
            {
                var sortOrder = pagination.OrderBy == "desc" ? "desc" : "asc";

                switch (pagination.ColumnToSort.ToLower())
                {
                    case "username":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.FirstName) : query.OrderByDescending(c => c.FirstName);
                        break;
                    case "usercode":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.LastName) : query.OrderByDescending(c => c.LastName);
                        break;
                    //Another case here for category
                    default:
                        break;
                }
            }

            //// Select Club
            //if (pagination.ClubQuery != null || pagination.ClubQuery != 0)
            //{
            //    var clubQuery = pagination.ClubQuery;
            //    query = query.Where(c => c.Club == clubQuery);
            //}

            var totalItemCount = await query.CountAsync();
            var totalPages = (int)Math.Ceiling((double)totalItemCount / pagination.PageSize);

            var usersList = await query
                .Skip((pagination.PageNumber - 1) * pagination.PageSize)
                .Take(pagination.PageSize)
                .ToListAsync();

            return (usersList, totalPages);
        }

        public async Task<User> GetUserByIdAsync(string id)
        {
            var getUser = new User();
            getUser = await _dbContext.Users.Where(x => x.EmployeeNumber == id).FirstAsync();
            if (getUser == null)
            {
                return new User();
            }
            return getUser;
        }

        public async Task<List<User>> GetUsersFullListAsync()
        {
            try
            {
                var result = await _dbContext.Users
                    .Where(x => x.RoleId != 4)
                    .Join(_dbContext.Roles, x => x.RoleId, y => y.Id, (x, y) => new { x, y })
                    .Join(_dbContext.Locations, xy => xy.x.Club, z => z.LocationCode, (xy, z) => new { xy.x, xy.y, z })
                    .Select(n => new User
                    {
                        Id = n.x.Id,
                        Username = n.x.Username,
                        EmployeeNumber = n.x.EmployeeNumber,
                        FirstName = n.x.FirstName,
                        MiddleName = n.x.MiddleName,
                        LastName = n.x.LastName,
                        Salt = n.x.Salt,
                        Hash = n.x.Hash,
                        Club = n.x.Club,
                        RoleId = n.x.RoleId,
                        IsLogin = n.x.IsLogin,
                        IsFirstLogin = n.x.IsFirstLogin,
                        Status = n.x.Status,
                        Attempt = n.x.Attempt
                    })
                .ToListAsync();
                return result;
            }
            catch (Exception ex)
            {
                // Log the exception details
                Console.WriteLine($"An error occurred: {ex.Message}");
                // Optionally, rethrow or handle the exception as needed
                throw;
            }
        }

        public async Task<User> InsertUserAsync(User user)
        {
            var MaxNumber = await _dbContext.Users
                .OrderByDescending(n => n.EmployeeNumber)
                .Select(n => new User 
                {
                    EmployeeNumber = n.EmployeeNumber
                }).FirstOrDefaultAsync();
            int NewEmployeeNumber = Convert.ToInt32(MaxNumber.EmployeeNumber) + 1;
            int salt = Convert.ToInt32(saltiness);
            int iterations = Convert.ToInt32(nIterations);

            var checkUser = new User();
            checkUser = await _dbContext.Users.Where(x => x.Username == user.Username).FirstOrDefaultAsync();
            if (checkUser == null)
            {
                // Define a lambda expression for inserting the user
                Func<User, Task<User>> insertUserLambda = async newUser =>
                {
                    var SaltPassword = _passwordHashService.GenerateSalt(70);
                    var HashedPassword = _passwordHashService.HashPassword("snr@dev", SaltPassword, iterations, salt);
                    var getUser = new User
                    {
                        EmployeeNumber = NewEmployeeNumber.ToString(),
                        FirstName = newUser.FirstName,
                        LastName = newUser.LastName,
                        MiddleName = newUser.MiddleName,
                        Username = newUser.Username,
                        Club = newUser.Club,
                        RoleId = newUser.RoleId,
                        Status = newUser.Status,
                        Salt = SaltPassword,
                        Hash = HashedPassword,
                        IsLogin = false,
                        IsFirstLogin = true,
                        Attempt = 0
                    };

                    await _dbContext.Users.AddAsync(getUser);
                    await _dbContext.SaveChangesAsync(); // Ensure changes are saved to the database
                    return getUser;
                };

                // Invoke the lambda to insert the user
                return await insertUserLambda(user);
            }

            return null;
        }


        public async Task<User> UpdateUserByIdAsync(User user)
        {
            var getUser = await _dbContext.Users.SingleOrDefaultAsync(x => x.EmployeeNumber == user.EmployeeNumber);
            
            if (getUser != null)
            {
                getUser.FirstName = user.FirstName;
                getUser.MiddleName = user.MiddleName;
                getUser.LastName = user.LastName;
                getUser.Username = user.Username;
                getUser.Club = user.Club;
                getUser.RoleId = user.RoleId;
                getUser.Status = user.Status;
                await _dbContext.SaveChangesAsync();

                return getUser;
            }
            else
            {
                return new User();
            }
        }

        public async Task<bool> ResetUserPasswordByIdAsync(string id)
        {
            int salt = Convert.ToInt32(saltiness);
            int iterations = Convert.ToInt32(nIterations);
            var SaltPassword = _passwordHashService.GenerateSalt(70);
            var HashedPassword = _passwordHashService.HashPassword("snr@dev", SaltPassword, iterations, salt);
            var getUser = await _dbContext.Users.SingleOrDefaultAsync(x => x.EmployeeNumber == id);

            if (getUser != null)
            {
                getUser.Salt = SaltPassword;
                getUser.Hash = HashedPassword;
                getUser.Status = true;
                getUser.IsFirstLogin = true;
                getUser.Attempt = 0;
                await _dbContext.SaveChangesAsync();

                return false;
            }
            else
            {
                return true;
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
