require "qpx/version"
require 'restclient'
require 'restclient/components'
require 'json'
require 'rack/cache'
require 'logger'
require 'moped'

module Qpx
  class Api

    RestClient.enable Rack::CommonLogger, STDOUT
    RestClient.enable Rack::Cache

    @@logger = Logger.new(STDOUT)
    @@logger.level = Logger::DEBUG
    @@logger.formatter = proc do |severity, datetime, progname, msg|
        "#{datetime}[QPX/#{progname}]- #{severity}: #{msg}\n"
    end
    # Configuration defaults
    @@config = {
      base_headers: {content_type: :json, accept_encoding: :gzip, user_agent: :qpx_gem}, #, accept: :json
      trips_url: 'https://www.googleapis.com/qpxExpress/v1/trips/search',
      mongo_url: ENV['MONGO_URL'],
      mongo_db_name: ENV['QPX_MONGO_DB_NAME'],
      google_api_key: ENV['GOOGLE_API_KEY'],
      mongo_airports_coll: 'qpx_airports',
      mongo_airlines_coll: 'qpx_airlines',
      mongo_trips_coll: 'qpx_trips',
      airports_filepath: File.expand_path('../../data/airports.dat', __FILE__),
      airlines_filepath: File.expand_path('../../data/airlines.dat', __FILE__),
      place_availables_mean: 5,
      max_solutions: 3
    }

    def self.config
      @@config
    end

    def self.logger
      @@logger
    end

    def initialize()
      puts "QPX Api Initialized"
    end

    def self.loadAirlinesData()
      return if @@config[:mongo_db][@@config[:mongo_airlines_coll]].find.count > 0
      @@logger.info("Reloading airlines data")
      File.open(@@config[:airlines_filepath], "r") do |f|
        f.each_line do |line|
          #id,name,alias,iata_code,icao_code,call_sign,country,active
          fields = line.split(',')
          @@config[:mongo_db][@@config[:mongo_airlines_coll]].insert({
             name:                fields[1].gsub('"',''),
             alias:               fields[2].gsub('"',''),
             iata_code:           fields[3].gsub('"',''),
             icao_code:           fields[4].gsub('"',''),
             call_sign:           fields[5].gsub('"',''),
             country:             fields[6].gsub('"',''),
             active:              fields[7].gsub('"','')
          })
        end
      end
    end


    def self.loadAirportsData()
      return if @@config[:mongo_db][@@config[:mongo_airports_coll]].find.count > 0
      @@logger.info("Reloading airports data")
      File.open(@@config[:airports_filepath], "r") do |f|
        f.each_line do |line|
          #id,name,city,country,iataCode,icao,latitude,longitude,altitude,utc_timezone_offset,daily_save_time,timezone
          fields = line.split(',')
          @@config[:mongo_db][@@config[:mongo_airports_coll]].insert({
             name:                fields[1].gsub('"',''),
             city:                fields[2].gsub('"',''),
             country:             fields[3].gsub('"',''),
             iata_code:           fields[4].gsub('"',''),
             icao:                fields[5].gsub('"',''),
             latitude:            fields[6].to_f,
             longitude:           fields[7].to_f,
             altitude:            fields[8].to_f,
             utc_timezone_offset: fields[9].to_f,
             daily_save_time:     fields[10].gsub('"',''),
             timezone:            fields[11].gsub('"',''),
             city_airport:            (fields[1].gsub('"','')=='All Airports')?true:false,
             first_class:         false
          }) unless fields[4].gsub('"','').lstrip =='' #avoid airport without iata code. its useless.
        end
      end
    end


    ####################################### Configuration Helpers ####################################
    @valid_config_keys = @@config.keys

    # Configure through hash
    def self.configure(opts = {})
      opts.each { |k, v| @@config[k.to_sym] = v if @valid_config_keys.include? k.to_sym }

      @@config[:mongo_db] = session = Moped::Session.new([@@config[:mongo_url]])
      @@config[:mongo_db].use @@config[:mongo_db_name]
      @@config[:mongo_db].login(@@config[:mongo_username], @@config[:mongo_password]) unless @@config[:mongo_username].nil?
      @@config[:mongo_db][@@config[:mongo_trips_col]].indexes.create({
        start_airport_code:  1,
        end_airport_code:    1,
        price:               1,
        departure:           1,
        arrival:             1,
        stopover:            1,
        company:             1 } ,
        { unique: true, dropDups: true, sparse: true })

      #Load general Data
      self.loadAirlinesData
      self.loadAirportsData
      'QPX is Configured and ready !'
    end


    ####################################### API Calls ####################################
    def self.search_trips(departure_code, arrival_code, outbound_date, inbound_date, adults_count, max_price=600)
      json_post_body = %Q!
      {
        "request": {
          "slice": [
            {
              "origin": "#{departure_code}",
              "destination": "#{arrival_code}",
              "date": "#{outbound_date.strftime('%Y-%m-%d')}"
            }
      !
      unless inbound_date.nil?
        json_post_body += %Q!
        ,{
          "destination": "#{departure_code}",
          "origin": "#{arrival_code}",
          "date": "#{inbound_date.strftime('%Y-%m-%d')}"
        }
        !
      end
      json_post_body += %Q!
           ],
          "passengers": {
            "adultCount": #{adults_count},
            "infantInLapCount": 0,
            "infantInSeatCount": 0,
            "childCount": 0,
            "seniorCount": 0
          },
          "maxPrice": "USD#{max_price}",
          "saleCountry": "USA",
          "solutions": #{@@config[:max_solutions]},
          "refundable": false
        }
      }
      !
      #@@logger.debug(json_post_body)
      begin
        response = RestClient.post(@@config[:trips_url], json_post_body, {
          params: {
            key: @@config[:google_api_key],
            fields: 'trips/tripOption(saleTotal,slice(duration,segment))'
          }
        }.merge(@@config[:base_headers]))

        if (response.code == 200)
          #@@logger.debug(response.body)
          data = JSON.parse(response.body)
          self.parseResponse(data)
        end
      rescue Exception => e
        @@logger.error( e.message)
      end
    end



    def self.parseResponse(data)
      #@@logger.debug(data)
      unless data.nil? or data == {}
        #aircrafts = data['trips']['data']['aircraft']
        #taxes     = data['trips']['data']['tax']
        #carriers  = data['trips']['data']['carrier']
        #airports  = data['trips']['data']['airport']
        trips     = data['trips']['tripOption']
        @@logger.info "#{trips.count} trips found."
        trips.each do |trip|
          firstSegment          = trip['slice'].first['segment'].first
          lastSegment           = trip['slice'].last['segment'].last
          firstSliceLastSegment = trip['slice'].first['segment'].last
          firstLeg              = firstSegment['leg'].first
          lastLeg               = lastSegment['leg'].last
          firstSliceLastLeg     = firstSliceLastSegment['leg'].last
          start_airport_code    = firstLeg['origin']
          end_airport_code      = firstSliceLastLeg['destination']
          start_airport_data    = @@config[:mongo_db][@@config[:mongo_airports_coll]].find({iata_code: start_airport_code}).to_a[0]
          end_airport_data      = @@config[:mongo_db][@@config[:mongo_airports_coll]].find({iata_code: end_airport_code}).to_a[0]
          first_company         = @@config[:mongo_db][@@config[:mongo_airlines_coll]].find({iata_code: firstSegment['flight']['carrier']}).to_a[0]['name']
          city_top_airport      = self.city_top_airport(end_airport_data['city'])
          begin
            @@config[:mongo_db][@@config[:mongo_trips_coll]].insert({
                      start_city: start_airport_data['city'],
                        end_city: end_airport_data['city'],
                     end_country: end_airport_data['country'],
                           price: trip['saleTotal'].sub('USD','').to_f,
               places_availables: @@config[:place_availables_mean], # Use a mean
                           about: '', # Description on town
                       departure: Time.parse(firstLeg['departureTime']),
                         arrival: Time.parse(lastLeg['arrivalTime']),
                        stopover: trip['slice'].inject(0) {|sum, slice| sum + slice['segment'].length },
                         company: first_company,
                         lowcost: false,
                            type: 'air', # Evol
                   start_airport: start_airport_data['name'],
              start_airport_code: start_airport_code,
                end_airport_code: end_airport_code,
                     end_airport: end_airport_data['city'],
                     coordinates: city_top_airport.values_at('longitude','latitude'),
                           title: '', # Evol
                        prefered: false,
                        start_time: Time.parse(firstLeg['departureTime']).strftime('%H.%M').to_f, #conserve current grapy system
                        end_time: Time.parse(lastLeg['arrivalTime']).strftime('%H.%M').to_f,
                        duration: trip['slice'].inject(0) { |duration, d| duration + d['duration'] }, #Can be computed again from start_time and end_time
                     search_date: Time.now,
                      airport_id: city_top_airport['_id']
                              })
          rescue Moped::Errors::OperationFailure => e
            @@logger.error('Insertion error. may be data is duplicated.')
          end
        end
      end
    end

    def self.multi_search_trips(departure_code, outbound_date, inbound_date, adults_count,max_price=600)
      @@logger.info "Searching from #{departure_code}"
      first_class_arrivals = @@config[:mongo_db][@@config[:mongo_airports_coll]].find(
        {first_class: true, iata_code: {'$nin' => [nil,'',departure_code]}}).select(iata_code: 1, _id: 0)
      first_class_arrivals.each do | first_class_arrival |
        puts "Searching #{departure_code} --> #{first_class_arrival['iata_code']} ..."
        self.search_trips(departure_code, first_class_arrival['iata_code'], outbound_date, inbound_date, adults_count,max_price)
      end
      "Done. #{first_class_arrivals.count} routes searched."
    end

    def self.multi_search_trips_by_city(departure_city, outbound_date, inbound_date, adults_count,max_price=600)
      city_airport = self.city_airport(departure_city)
      if city_airport.nil? or city_airport.empty?
        @@logger.warn "No top airport found for city #{departure_city}; Will search all aiports in the city."
        @@config[:mongo_db][@@config[:mongo_airports_coll]].find({city: departure_city, iata_code: {'$nin' => [nil,'']}}).each do |any_city_airport|
          self.multi_search_trips( any_city_airport['iata_code'], outbound_date, inbound_date, adults_count,max_price)
        end
      else
        self.multi_search_trips( city_airport['iata_code'], outbound_date, inbound_date, adults_count,max_price)
      end
    end

    def self.city_airport(city)
      @@config[:mongo_db][@@config[:mongo_airports_coll]].find({city: city, city_airport: true, iata_code: {'$nin' => [nil,'']}}).limit(1).one
    end

    def self.city_top_airport(city)
      @@config[:mongo_db][@@config[:mongo_airports_coll]].find({city: city,iata_code: {'$nin' => [nil,'']}}).sort({city_airport: -1}).limit(1).one
    end

  end
end
