using CSI.Application.DTOs;
using CSI.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Infrastructure.Data
{
    public class AppDBContext : DbContext
    {
        public AppDBContext(DbContextOptions<AppDBContext> options) : base(options)
        {
            Users = Set<User>();
            Departments = Set<Department>();
            CustomerCodes = Set<CustomerCodes>();
            Analytics = Set<Analytics>();
            Prooflist = Set<Prooflist>();
            Locations = Set<Location>();
            Status = Set<Status>();
            Adjustments = Set<Adjustments>();
            AnalyticsProoflist = Set<AnalyticsProoflist>();
            Actions = Set<Actions>();
            Reasons = Set<Reasons>();
            Match = Set<Match>();
            AccountingMatch = Set<AccountingMatch>();
            Roles = Set<Roles>();
            AnalyticsView = Set<AnalyticsView>();
            Source = Set<Source>();
            AdjustmentExceptions = Set<AdjustmentExceptions>();
            GenerateInvoice = Set<GenerateInvoice>();
            Reference = Set<Reference>();
            DashboardAccounting = Set<DashboardAccounting>();
            FileDescription = Set<FileDescriptions>();
            AccountingProoflists = Set<AccountingProoflist>();
            Logs = Set<Logs>();
            Category = Set<Category>();
            VarianceMMS = Set<VarianceMMS>();
        }

        public DbSet<User> Users { get; set; }
        public DbSet<Department> Departments { get; set; }
        public DbSet<CustomerCodes> CustomerCodes { get; set; }
        public DbSet<Analytics> Analytics { get; set; }
        //public DbSet<SumMMS> SumMMS { get; set; }
        public DbSet<Prooflist> Prooflist { get; set; }
        public DbSet<Location> Locations { get; set; }
        public DbSet<Status> Status { get; set; }
        public DbSet<Adjustments> Adjustments { get; set; }
        public DbSet<AnalyticsProoflist> AnalyticsProoflist { get; set; }
        public DbSet<Actions> Actions { get; set; }
        public DbSet<Reasons> Reasons { get; set; }
        public DbSet<Match> Match { get; set; }
        public DbSet<AccountingMatch> AccountingMatch { get; set; }
        public DbSet<Roles> Roles { get; set; }
        public DbSet<AnalyticsView> AnalyticsView { get; set; }
        public DbSet<Source> Source { get; set; }
        public DbSet<AdjustmentExceptions> AdjustmentExceptions { get; set; }
        public DbSet<GenerateInvoice> GenerateInvoice { get; set; }
        public DbSet<Reference> Reference { get; set; }
        public DbSet<DashboardAccounting> DashboardAccounting { get; set; }
        public DbSet<FileDescriptions> FileDescription { get; set; }
        public DbSet<AccountingProoflist> AccountingProoflists { get; set; }
        public DbSet<Logs> Logs { get; set; }
        public DbSet<Category> Category { get; set; }
        public DbSet<VarianceMMS> VarianceMMS { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<User>()
            .ToTable("tbl_user");

            modelBuilder.Entity<Department>()
            .ToTable("tbl_department_code");

            modelBuilder.Entity<CustomerCodes>()
            .ToTable("tbl_customer");

            modelBuilder.Entity<Analytics>()
            .ToTable("tbl_analytics");

            modelBuilder.Entity<Prooflist>()
            .ToTable("tbl_prooflist");

            modelBuilder.Entity<Location>()
            .ToTable("tbl_location");

            modelBuilder.Entity<Status>()
            .ToTable("tbl_status");

            modelBuilder.Entity<Adjustments>()
            .ToTable("tbl_adjustments");

            modelBuilder.Entity<AnalyticsProoflist>()
            .ToTable("tbl_analytics_prooflist");

            modelBuilder.Entity<Actions>()
            .ToTable("tbl_action");

            modelBuilder.Entity<Reasons>()
            .ToTable("tbl_reason");

            modelBuilder.Entity<Match>()
            .HasNoKey();

            modelBuilder.Entity<AccountingMatch>()
           .HasNoKey();

            modelBuilder.Entity<AnalyticsView>()
            .HasNoKey();

            modelBuilder.Entity<AdjustmentExceptions>()
            .HasNoKey();

            modelBuilder.Entity<DashboardAccounting>()
            .HasNoKey();

            modelBuilder.Entity<Roles>()
            .ToTable("tbl_role");

            modelBuilder.Entity<Source>()
            .ToTable("tbl_source");

            modelBuilder.Entity<GenerateInvoice>()
           .ToTable("tbl_generated_invoice");

            modelBuilder.Entity<Reference>()
           .ToTable("tbl_reference");

            modelBuilder.Entity<FileDescriptions>()
            .ToTable("tbl_file_descriptions");

            modelBuilder.Entity<AccountingProoflist>()
           .ToTable("tbl_accounting_prooflist");

            modelBuilder.Entity<Logs>()
            .ToTable("tbl_logs");

            modelBuilder.Entity<Category>()
            .ToTable("tbl_category");

            modelBuilder.Entity<VarianceMMS>()
            .HasNoKey();
        }
    }
}
