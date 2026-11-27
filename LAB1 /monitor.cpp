#include <pthread.h>
#include <unistd.h>
#include <iostream>

class Event {
public:
    explicit Event(int id) : id(id) {}
    int id;
};

class Monitor {
public:
    bool provide(Event* event) {
        pthread_mutex_lock(&mutex);

        if (ready) {
            pthread_mutex_unlock(&mutex);
            return false;
        }

        data = event;
        ready = true;
        std::cout << "[Поставщик] Отправлено: " << event->id << std::endl;
        
        pthread_cond_signal(&condition);
        pthread_mutex_unlock(&mutex);
        return true;
    }

    bool consume() {
        pthread_mutex_lock(&mutex);

        while (!ready && !stopping) {
            pthread_cond_wait(&condition, &mutex);
        }

        if (stopping && !ready) {
            pthread_mutex_unlock(&mutex);
            return false;
        }

        Event* event = static_cast<Event*>(data);
        data = nullptr;
        ready = false;

        std::cout << "[Потребитель] Обработано: " << event->id << std::endl;
        delete event;
        pthread_mutex_unlock(&mutex);
        
        return true;
    }

    void stop() {
        pthread_mutex_lock(&mutex);
        stopping = true;
        pthread_cond_broadcast(&condition);
        pthread_mutex_unlock(&mutex);
    }

private:
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t condition = PTHREAD_COND_INITIALIZER;
    bool ready = false;
    bool stopping = false; 
    void* data = nullptr;
};

void* producer(void* arg) {
    Monitor* monitor = static_cast<Monitor*>(arg);
    for (int id = 1; id <= 5; ++id) {
        sleep(1);
        Event* new_event = new Event(id);
        while (!monitor->provide(new_event)) {
            sleep(1);
        }
    }
    monitor->stop();
    return nullptr;
}

void* consumer(void* arg) {
    Monitor* monitor = static_cast<Monitor*>(arg);
    while (monitor->consume()) {
    
    }
    return nullptr;
}

int main() {
    Monitor monitor;
    pthread_t producer_thread, consumer_thread;

    pthread_create(&consumer_thread, nullptr, consumer, &monitor);
    pthread_create(&producer_thread, nullptr, producer, &monitor);

    pthread_join(producer_thread, nullptr);
    pthread_join(consumer_thread, nullptr);

    return 0;
}
