﻿// <auto-generated />
using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;
using vgt_saga_flight.Models;

#nullable disable

namespace vgt_saga_flight.Migrations
{
    [DbContext(typeof(FlightDbContext))]
    [Migration("20240512193555_Initial")]
    partial class Initial
    {
        /// <inheritdoc />
        protected override void BuildTargetModel(ModelBuilder modelBuilder)
        {
#pragma warning disable 612, 618
            modelBuilder
                .HasAnnotation("ProductVersion", "8.0.4")
                .HasAnnotation("Relational:MaxIdentifierLength", 63);

            NpgsqlModelBuilderExtensions.UseIdentityByDefaultColumns(modelBuilder);

            modelBuilder.Entity("vgt_saga_flight.Models.AirportDb", b =>
                {
                    b.Property<int>("AirportDbId")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("integer");

                    NpgsqlPropertyBuilderExtensions.UseIdentityByDefaultColumn(b.Property<int>("AirportDbId"));

                    b.Property<string>("AirportCity")
                        .IsRequired()
                        .HasColumnType("text");

                    b.Property<string>("AirportCode")
                        .IsRequired()
                        .HasColumnType("text");

                    b.Property<bool>("IsDeparture")
                        .HasColumnType("boolean");

                    b.HasKey("AirportDbId");

                    b.ToTable("Airports");
                });

            modelBuilder.Entity("vgt_saga_flight.Models.Booking", b =>
                {
                    b.Property<int>("BookingId")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("integer");

                    NpgsqlPropertyBuilderExtensions.UseIdentityByDefaultColumn(b.Property<int>("BookingId"));

                    b.Property<int>("FlightDbId")
                        .HasColumnType("integer");

                    b.Property<int>("Temporary")
                        .HasColumnType("integer");

                    b.Property<DateTime>("TemporaryDt")
                        .HasColumnType("timestamp with time zone");

                    b.Property<Guid>("TransactionId")
                        .HasColumnType("uuid");

                    b.HasKey("BookingId");

                    b.HasIndex("FlightDbId");

                    b.ToTable("Bookings");
                });

            modelBuilder.Entity("vgt_saga_flight.Models.FlightDb", b =>
                {
                    b.Property<int>("FlightDbId")
                        .ValueGeneratedOnAdd()
                        .HasColumnType("integer");

                    NpgsqlPropertyBuilderExtensions.UseIdentityByDefaultColumn(b.Property<int>("FlightDbId"));

                    b.Property<int>("Amount")
                        .HasColumnType("integer");

                    b.Property<int>("ArrivalAirportAirportDbId")
                        .HasColumnType("integer");

                    b.Property<int>("DepartureAirportAirportDbId")
                        .HasColumnType("integer");

                    b.Property<DateTime>("FlightTime")
                        .HasColumnType("timestamp with time zone");

                    b.HasKey("FlightDbId");

                    b.HasIndex("ArrivalAirportAirportDbId");

                    b.HasIndex("DepartureAirportAirportDbId");

                    b.ToTable("Flights");
                });

            modelBuilder.Entity("vgt_saga_flight.Models.Booking", b =>
                {
                    b.HasOne("vgt_saga_flight.Models.FlightDb", "Flight")
                        .WithMany()
                        .HasForeignKey("FlightDbId")
                        .OnDelete(DeleteBehavior.Cascade)
                        .IsRequired();

                    b.Navigation("Flight");
                });

            modelBuilder.Entity("vgt_saga_flight.Models.FlightDb", b =>
                {
                    b.HasOne("vgt_saga_flight.Models.AirportDb", "ArrivalAirport")
                        .WithMany()
                        .HasForeignKey("ArrivalAirportAirportDbId")
                        .OnDelete(DeleteBehavior.Cascade)
                        .IsRequired();

                    b.HasOne("vgt_saga_flight.Models.AirportDb", "DepartureAirport")
                        .WithMany()
                        .HasForeignKey("DepartureAirportAirportDbId")
                        .OnDelete(DeleteBehavior.Cascade)
                        .IsRequired();

                    b.Navigation("ArrivalAirport");

                    b.Navigation("DepartureAirport");
                });
#pragma warning restore 612, 618
        }
    }
}
