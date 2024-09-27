using CSI.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Infrastructure.Data
{
    public class AppOmsDBContext : DbContext
    {
        public AppOmsDBContext(DbContextOptions<AppOmsDBContext> options):base(options)
        {
            Saleshead = Set<Saleshead>();
        }
        public DbSet<Saleshead> Saleshead { get; set; }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Saleshead>().HasNoKey();
        }
    }
}
