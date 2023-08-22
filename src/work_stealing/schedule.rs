use std::sync::{atomic::{AtomicUsize, Ordering}, Mutex};

pub trait Task {
    fn execute(&self);
}

struct Tasks;

impl Task for Tasks {
    fn execute(&self) {
        println!("execute");
    }
}

type Buffer<T> = Vec<Option<Box<T>>>;

#[derive(Debug)]
pub struct WorkStealingDeque<T>
where
    T: Task,
{
    buffer: Mutex<Buffer<T>>,
}

impl<T> WorkStealingDeque<T>
where
    T: Task,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(Buffer::with_capacity(capacity)),
        }
    }

    pub fn push(&mut self, task: Box<T>) {
        let mut buffer = self.buffer.lock().unwrap();

        buffer.push(Some(task));
    }

    pub fn pop(&mut self) -> Option<Box<T>> {
        let mut buffer = self.buffer.lock().unwrap();

        while let Some(slot) = buffer.pop() {
            if slot.is_some() {
                return slot;
            }
        }

        None
    }

    pub fn steal(&mut self) -> Option<Box<T>> {
        let mut buffer = self.buffer.lock().unwrap();

        for slot in buffer.iter_mut().rev() {
            if slot.is_some() {
                return slot.take();
            }
        }

        None
    }
}

#[cfg(test)]
mod work_steal_schedule_test {
    use super::*;

    struct TestTask(pub u32);

    impl Task for TestTask {
        fn execute(&self) {
            println!("execute {}", self.0);
        }
    }

    #[test]
    fn test_push_pop() {
        let mut deque: WorkStealingDeque<TestTask> = WorkStealingDeque::new(10);

        deque.push(Box::new(TestTask(1)));
        assert!(deque.pop().is_some());

        deque.push(Box::new(TestTask(2)));
        assert!(deque.pop().is_some());

        assert!(deque.pop().is_none());
    }
    
    #[test]
    fn test_steal() {
        let mut deque: WorkStealingDeque<TestTask> = WorkStealingDeque::new(10);

        deque.push(Box::new(TestTask(1)));
        deque.push(Box::new(TestTask(2)));
        deque.push(Box::new(TestTask(3)));

        assert_eq!(deque.steal().map(|task| task.0), Some(3));
        assert_eq!(deque.steal().map(|task| task.0), Some(2));
        assert_eq!(deque.steal().map(|task| task.0), Some(1));
        assert!(deque.steal().is_none());
    }
}