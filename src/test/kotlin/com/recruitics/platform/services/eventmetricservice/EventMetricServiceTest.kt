package com.recruitics.platform.services.eventmetricservice

import com.nhaarman.mockitokotlin2.*
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux
import reactor.core.publisher.toMono
import java.time.LocalDate

internal class EventMetricServiceTest {
  @Test
  internal fun `test creation of metrics, with no aggregate pre-existing`() {
    val eventMetricRepository = mock<EventMetricRepository>()
    val metricEntryRepository = mock<MetricEntryRepository>()

    whenever(eventMetricRepository.findById("1-${LocalDate.now()}")).thenReturn(Mono.empty())
    whenever(eventMetricRepository.save(any<EventMetricAggregate>())).then { (it.arguments[0] as EventMetricAggregate).toMono() }
    whenever(metricEntryRepository.saveAll(any<Iterable<MetricEntry>>())).then { (it.arguments[0] as Iterable<MetricEntry>).toFlux() }
    whenever(metricEntryRepository.deleteAll(any<Iterable<MetricEntry>>())).then { Mono.empty<Void>() }

    val service = EventMetricService(eventMetricRepository, metricEntryRepository)

    val entry = MetricEntry(
      MetricEntryKey("source", "medium", "adId", "eventType", true),
      0,
      0
    )
    service.saveMetricEntries(1, LocalDate.now(), listOf(entry)).collectList().block()

    verify(eventMetricRepository).save(eq(EventMetricAggregate(1, LocalDate.now(), listOf(entry))))
    verify(metricEntryRepository).saveAll(listOf(entry))
  }

  @Test
  internal fun `test creation of metrics with pre-existing aggregate`() {
    val eventMetricRepository = mock<EventMetricRepository>()
    val metricEntryRepository = mock<MetricEntryRepository>()

    val metricAggregate = EventMetricAggregate(1,LocalDate.now(), emptyList())

    whenever(eventMetricRepository.findById("1-${LocalDate.now()}")).thenReturn(Mono.just(metricAggregate))
    whenever(eventMetricRepository.save(any<EventMetricAggregate>())).then { (it.arguments[0] as EventMetricAggregate).toMono() }
    whenever(metricEntryRepository.saveAll(any<Iterable<MetricEntry>>())).then { (it.arguments[0] as Iterable<MetricEntry>).toFlux() }
    whenever(metricEntryRepository.deleteAll(any<Iterable<MetricEntry>>())).then { Mono.empty<Void>() }

    val service = EventMetricService(eventMetricRepository, metricEntryRepository)

    val entry = MetricEntry(
      MetricEntryKey("source", "medium", "adId", "eventType", true),
      0,
      0
    )
    service.saveMetricEntries(1, LocalDate.now(), listOf(entry)).collectList().block()

    verify(eventMetricRepository).save(eq(EventMetricAggregate(1, LocalDate.now(), listOf(entry))))
    verify(metricEntryRepository).saveAll(listOf(entry))
  }

  @Test
  internal fun `test replacing metrics`() {
    val eventMetricRepository = mock<EventMetricRepository>()
    val metricEntryRepository = mock<MetricEntryRepository>()

    val entry = MetricEntry(
      MetricEntryKey("source", "medium", "adId", "eventType", true),
      0,
      0
    )

    val metricAggregate = EventMetricAggregate(1,LocalDate.now(), listOf(entry))

    whenever(eventMetricRepository.findById("1-${LocalDate.now()}")).thenReturn(Mono.just(metricAggregate))
    whenever(metricEntryRepository.deleteAll(any<Iterable<MetricEntry>>())).then { Mono.empty<Void>() }
    whenever(eventMetricRepository.save(any<EventMetricAggregate>())).then { (it.arguments[0] as EventMetricAggregate).toMono() }
    whenever(metricEntryRepository.saveAll(any<Iterable<MetricEntry>>())).then { (it.arguments[0] as Iterable<MetricEntry>).toFlux() }

    val service = EventMetricService(eventMetricRepository, metricEntryRepository)

    val newEntry = MetricEntry(
      MetricEntryKey("source", "medium", "adId", "eventType", true),
      1,
      1
    )

    service.saveMetricEntries(1, LocalDate.now(), listOf(newEntry)).collectList().block()

    verify(eventMetricRepository).save(eq(EventMetricAggregate(1, LocalDate.now(), listOf(newEntry))))
    verify(metricEntryRepository).deleteAll(listOf(entry))
    verify(metricEntryRepository).saveAll(listOf(newEntry))
  }
}