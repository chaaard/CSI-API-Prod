using AutoMapper;
using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using NetTopologySuite.Geometries;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Services
{
    public class LocationService : ILocationService
    {
        private readonly AppDBContext _dbContext;
        private readonly IConfiguration _configuration;
        private readonly IMapper _mapper;

        public LocationService(IConfiguration configuration, AppDBContext dBContext, IMapper mapper)
        {
            _configuration = configuration;
            _dbContext = dBContext;
            _mapper = mapper;
        }

        public async Task<List<Domain.Entities.Location>> GetLocation()
        {
            var getLocations = new List<Domain.Entities.Location>();
            getLocations = await _dbContext.Locations.ToListAsync();
            return getLocations;
        }

        public async Task<(List<LocationDto>, int totalPages)> GetLocationsAsync(PaginationDto pagination)
        {
            var query = _dbContext.Locations
                .Select(n => new LocationDto
                {
                    Id = n.Id,
                    LocationCode = n.LocationCode,
                    LocationName = n.LocationName,
                    //LocationName = n.LocationName.Replace("KAREILA", n.LocationCode.ToString()).Trim(),
                    ShortName = n.ShortName,
                    DeleteFlag = n.DeleteFlag,
                })
                .Where(x => (EF.Functions.Like(x.LocationName.ToLower(), $"%kareila%")))
                .AsQueryable();


            // Searching
            if (!string.IsNullOrEmpty(pagination.SearchQuery))
            {
                var searchQuery = $"%{pagination.SearchQuery.ToLower()}%";

                query = query.Where(c =>
                    (EF.Functions.Like(c.LocationName.ToLower(), searchQuery)) ||
                    (EF.Functions.Like(c.ShortName.ToLower(), searchQuery))
                //Add the category column here
                );
            }

            // Sorting
            if (!string.IsNullOrEmpty(pagination.ColumnToSort))
            {
                var sortOrder = pagination.OrderBy == "desc" ? "desc" : "asc";

                switch (pagination.ColumnToSort.ToLower())
                {
                    case "locationcode":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.LocationCode) : query.OrderByDescending(c => c.LocationCode);
                        break;
                    case "locationname":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.LocationName) : query.OrderByDescending(c => c.LocationName);
                        break;
                    case "shortname":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.ShortName) : query.OrderByDescending(c => c.ShortName);
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

            var locationList = await query
                .Skip((pagination.PageNumber - 1) * pagination.PageSize)
                .Take(pagination.PageSize)
                .ToListAsync();

            return (locationList, totalPages);
        }

        public async Task<List<Domain.Entities.Location>> GetLocationDdCodesAsync()
        {
            var query = await _dbContext.Locations
                .ToListAsync();

            return query;
        }

        public async Task<Domain.Entities.Location> GetLocationByIdAsync(int Id)
        {
            var getLocations = new Domain.Entities.Location();
            getLocations = await _dbContext.Locations.Where(x => x.Id == Id).FirstAsync();
            return getLocations;
        }

        public async Task<Domain.Entities.Location> UpdateLocationByIdAsync(LocationDto location)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var getLocation = await _dbContext.Locations.SingleOrDefaultAsync(x => x.Id == location.Id);

                if (getLocation != null)
                {
                    var oldLocationCode = getLocation.LocationCode;
                    var oldLocationName = getLocation.LocationName;
                    var oldShortName = getLocation.ShortName;
                    var oldDeleteFlag = getLocation.DeleteFlag;
                    getLocation.LocationCode = location.LocationCode;
                    getLocation.LocationName = location.LocationName;
                    getLocation.ShortName = location.ShortName;
                    getLocation.DeleteFlag = location.DeleteFlag;
                    await _dbContext.SaveChangesAsync();

                    logsDto = new LogsDto
                    {
                        Date = DateTime.Now,
                        Action = "Update Club",
                        Remarks = $"Updated Successfully" +
                                  $"Id: {location.Id} : " +
                                  $"LocationCode: {oldLocationCode} -> {location.LocationCode}, " +
                                  $"LocationName: {oldLocationName} -> {location.LocationName}, " +
                                  $"ShortName: {oldShortName} -> {location.ShortName}, " +
                                  $"DeleteFlag: {oldDeleteFlag} -> {location.DeleteFlag}"
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

                    return getLocation;
                }
                else
                {
                    return new Domain.Entities.Location();
                }
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = location.UserId,
                    Date = DateTime.Now,
                    Action = "Update Club",
                    Remarks = $"Error: {ex.Message}",
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<Domain.Entities.Location> InsertLocationAsync(LocationDto location)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var getLocation = await _dbContext.Locations.SingleOrDefaultAsync(
                    x => x.LocationCode == location.LocationCode
                      || x.LocationName == location.LocationName
                      || x.ShortName == location.ShortName);

                if (getLocation == null)
                {
                    Func<LocationDto, Task<Domain.Entities.Location>> insertLocationLambda = async newLocation =>
                    {
                        var getLocation = new Domain.Entities.Location
                        {
                            LocationCode = newLocation.LocationCode,
                            LocationName = newLocation.LocationName,
                            ShortName = newLocation.ShortName,
                            DeleteFlag = newLocation.DeleteFlag,
                        };

                        await _dbContext.Locations.AddAsync(getLocation);
                        await _dbContext.SaveChangesAsync();
                        return getLocation;
                    };

                    logsDto = new LogsDto
                    {
                        UserId = location.UserId,
                        Date = DateTime.Now,
                        Action = "Insert Club",
                        Remarks = $"Successfully Added",
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();
                    return await insertLocationLambda(location);
                }
                return null;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = location.UserId,
                    Date = DateTime.Now,
                    Action = "Insert Club",
                    Remarks = $"Error: {ex.Message}",
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<bool> DeleteLocationByIdAsync(LocationDto location)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var getLocation = await _dbContext.Locations.SingleOrDefaultAsync(x => x.Id == location.Id);

                if (getLocation != null)
                {
                    getLocation.DeleteFlag = true;
                    await _dbContext.SaveChangesAsync();

                    logsDto = new LogsDto
                    {
                        UserId = location.UserId,
                        Date = DateTime.Now,
                        Action = "Delete Club",
                        Remarks = $"Successfully Deleted",
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

                    return true;
                }
                else
                {
                    logsDto = new LogsDto
                    {
                        UserId = location.UserId,
                        Date = DateTime.Now,
                        Action = "Delete Club",
                        Remarks = $"Club Not Found",
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();
                    return false;
                }
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = location.UserId,
                    Date = DateTime.Now,
                    Action = "Delete Club",
                    Remarks = $"Error: {ex.Message}",
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }

        }
    }
}
