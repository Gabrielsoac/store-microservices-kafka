package br.com.microservices.orchestrated.orderservice.core.service;

import br.com.microservices.orchestrated.orderservice.config.exception.ValidationException;
import br.com.microservices.orchestrated.orderservice.core.document.Event;
import br.com.microservices.orchestrated.orderservice.core.dto.EventFilters;
import br.com.microservices.orchestrated.orderservice.core.repository.EventRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@AllArgsConstructor
@Slf4j
public class EventService {
    private final EventRepository eventRepository;

    public Event save(Event event){
        return eventRepository.save(event);
    }

    public void notifyEnding(Event event){
        event.setCreatedAt(LocalDateTime.now());
        save(event);
        log.info("Order {} with saga notified! TransactionId: {}", event.getOrderId(), event.getTransactionId());
    }

    public List<Event> findAll(){
        return eventRepository.findAllByOrderByCreatedAtDesc();
    }

    public Event findByFilters(EventFilters eventFilters){
        validateFilters(eventFilters);
        if(eventFilters.getOrderId() != null && !eventFilters.getOrderId().isEmpty()) {
            return findByOrderId(eventFilters.getOrderId());
        }
        return findByTransactionId(eventFilters.getTransactionId());
    }

    private Event findByOrderId(String orderId){
        return eventRepository
                .findTop1ByOrderIdOrderByCreatedAtDesc(orderId)
                .orElseThrow(
                    () -> new ValidationException("Event not found by OrderID")
                );
    }

    private Event findByTransactionId(String transactionId){
        return eventRepository
                .findTop1ByTransactionIdOrderByCreatedAtDesc(transactionId)
                .orElseThrow(() -> new ValidationException("Event not found by TransactionID"));
    }

    private void validateFilters(EventFilters filters){
        boolean orderIdEmpty = filters.getOrderId() == null || filters.getOrderId().isEmpty();
        boolean transactionIdEmpty = filters.getTransactionId() == null || filters.getTransactionId().isEmpty();
        
        if(orderIdEmpty && transactionIdEmpty){
            throw new ValidationException("OrderID or TransactionID must be informed.");
        }
    }
}
