package com.recruitics.platform.services.eventmetricservice

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.annotation.JsonNaming
import io.swagger.v3.oas.annotations.OpenAPIDefinition
import io.swagger.v3.oas.annotations.info.Contact
import io.swagger.v3.oas.annotations.info.Info
import io.swagger.v3.oas.annotations.tags.Tag
import org.bson.types.ObjectId
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.cloud.client.discovery.EnableDiscoveryClient
import org.springframework.cloud.context.config.annotation.RefreshScope
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.DBRef
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.format.annotation.DateTimeFormat.ISO
import org.springframework.http.MediaType
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.*
import org.springframework.web.reactive.config.CorsRegistry
import org.springframework.web.reactive.config.WebFluxConfigurer
import reactor.core.publisher.Flux
import reactor.core.publisher.switchIfEmpty
import reactor.core.publisher.toMono
import java.time.LocalDate

@SpringBootApplication
@EnableDiscoveryClient
@EnableMongoRepositories
class EventMetricServiceApplication

fun main(args: Array<String>) {
  runApplication<EventMetricServiceApplication>(*args)
}

@Configuration
class CorsConfiguration {
  @Bean
  fun corsConfigurer(): WebFluxConfigurer = object : WebFluxConfigurer {
    override fun addCorsMappings(registry: CorsRegistry) {
      registry.addMapping("/**")
        .allowedOrigins("*")
        .allowedMethods("*")
        .maxAge(3600)
    }
  }
}

@RestController
@RequestMapping("/event-metrics")
@Tag(name = "Event Metric Service")
@OpenAPIDefinition(
  info = Info(
    title = "Event Metric Service",
    description = "API for creating and querying event metrics",
    contact = Contact(name = "Data Engineering", email = "data.engineering@recruitics.com")
  )
)
class EventMetricController(
  private val eventMetricService: EventMetricService
) {
  @GetMapping(produces = [MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_STREAM_JSON_VALUE])
  fun getPurchaseOrders(
    @RequestParam("client_id") clientId: Int,
    @RequestParam("start_date", required = false)
    @DateTimeFormat(iso = ISO.DATE)
    startDate: LocalDate?,
    @RequestParam("end_date", required = false)
    @DateTimeFormat(iso = ISO.DATE)
    endDate: LocalDate?
  ): Flux<EventMetricAggregate> {
    return eventMetricService.getMetricsForRange(clientId, startDate ?: LocalDate.now(), endDate ?: LocalDate.now())
  }

  @PostMapping(
    "{clientId}/{date}",
    consumes = [MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_STREAM_JSON_VALUE],
    produces = [MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_STREAM_JSON_VALUE]
  )
  fun createEventMetric(
    @PathVariable clientId: Int,
    @DateTimeFormat(iso = ISO.DATE)
    @PathVariable date: LocalDate,
    @RequestBody createEventMetric: CreateEventMetric
  ): Flux<MetricEntry> {
    return eventMetricService.saveMetricEntries(clientId, date, createEventMetric.toMetricEntry())
  }

  @PostMapping(
    "batch/{clientId}/{date}",
    consumes = [MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_STREAM_JSON_VALUE],
    produces = [MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_STREAM_JSON_VALUE]
  )
  fun createEventMetric(
    @PathVariable clientId: Int,
    @DateTimeFormat(iso = ISO.DATE)
    @PathVariable date: LocalDate,
    @RequestBody createEventMetric: List<CreateEventMetric>
  ): Flux<MetricEntry> {
    return eventMetricService.saveMetricEntries(clientId, date, createEventMetric.flatMap { it.toMetricEntry() })
  }
}

@Service
class EventMetricService
  (
  val eventMetricRepository: EventMetricRepository,
  val metricEntryRepository: MetricEntryRepository
) {
  fun getMetricsForRange(clientId: Int, startDate: LocalDate, endDate: LocalDate): Flux<EventMetricAggregate> {
    return eventMetricRepository.findAllById(
      generateSequence(startDate) { it.plusDays(1) }
        .takeWhile { it <= endDate }
        .map { "$clientId-$it" }
        .asIterable()
    )
  }

  fun saveMetricEntries(clientId: Int, date: LocalDate, metricEntries: List<MetricEntry>): Flux<MetricEntry> {
    return eventMetricRepository.findById("$clientId-$date")
      .switchIfEmpty {
        EventMetricAggregate(
          clientId,
          date,
          metrics = emptyList()
        ).toMono()
      }.flatMapMany { aggregate ->
        val entryKeys = metricEntries.map { it.key }
        val (keysToDelete, keysToKeep) = aggregate.metrics.partition { entryKeys.contains(it.key) }
        metricEntryRepository.deleteAll(keysToDelete)
          .then(metricEntryRepository.saveAll(metricEntries).collectList())
          .flatMapMany { newEntries ->
            val newAggregate = aggregate.copy(
              metrics = keysToKeep.plus(newEntries)
            )
            eventMetricRepository.save(newAggregate).flatMapIterable { newEntries }
          }
      }
  }
}


@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy::class)
data class CreateEventMetric(
  val source: String,
  val medium: String,
  val adId: String?,
  val eventType: String,
  val paidSpend: Long,
  val unPaidSpend: Long,
  val paidCount: Int,
  val unPaidCount: Int
)

fun CreateEventMetric.toMetricEntry() = listOf(
  MetricEntry(
    MetricEntryKey(source, medium, adId, eventType, paid = true),
    spend = paidSpend,
    count = paidCount
  ),
  MetricEntry(
    MetricEntryKey(source, medium, adId, eventType, paid = false),
    spend = unPaidSpend,
    count = unPaidCount
  )
)

@Repository
interface EventMetricRepository : ReactiveCrudRepository<EventMetricAggregate, String>

@Repository
interface MetricEntryRepository : ReactiveCrudRepository<MetricEntry, ObjectId>

@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy::class)
data class EventMetricAggregate(
  val clientId: Int,
  val date: LocalDate,
  @DBRef
  val metrics: List<MetricEntry>,
  @Id
  private val id: String = "$clientId-$date"
)

@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy::class)
data class MetricEntryKey(
  val source: String,
  val medium: String,
  val adId: String?,
  val eventType: String,
  val paid: Boolean
)

@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy::class)
data class MetricEntry(
  val key: MetricEntryKey,
  val spend: Long,
  val count: Int,
  @Id
  private val id: ObjectId? = null
)