#include <iostream>
#include <string>
#include <thread>
#include <boost/asio.hpp>
#include <atomic>

class ChatClient {
public:
    ChatClient() : socket_(io_context_), running_(false) {}

    bool connect(const std::string& host, unsigned short port) {
        try {
            boost::asio::ip::tcp::resolver resolver(io_context_);
            auto endpoints = resolver.resolve(host, std::to_string(port));
            
            boost::asio::connect(socket_, endpoints);
            std::cout << "서버에 연결되었습니다." << std::endl;
            
            running_ = true;
            return true;
        } catch (const std::exception& e) {
            std::cerr << "연결 실패: " << e.what() << std::endl;
            return false;
        }
    }

    void start() {
        // 서버로부터 메시지를 받는 스레드 시작
        receive_thread_ = std::thread([this]() {
            try {
                while (running_) {
                    boost::asio::streambuf buf;
                    boost::asio::read_until(socket_, buf, '\n');
                    std::string message;
                    std::istream is(&buf);
                    std::getline(is, message);
                    std::cout << message << std::endl;
                }
            } catch (const std::exception& e) {
                if (running_) {
                    std::cerr << "수신 오류: " << e.what() << std::endl;
                }
            }
        });

        // 사용자 입력을 처리하는 메인 스레드
        std::string input;
        while (running_) {
            std::getline(std::cin, input);
            if (input == "/quit") {
                break;
            }
            sendMessage(input);
        }

        stop();
    }

    void stop() {
        running_ = false;
        if (receive_thread_.joinable()) {
            receive_thread_.join();
        }
        if (socket_.is_open()) {
            socket_.close();
        }
    }

private:
    void sendMessage(const std::string& message) {
        try {
            boost::asio::write(socket_, boost::asio::buffer(message + "\n"));
        } catch (const std::exception& e) {
            std::cerr << "전송 오류: " << e.what() << std::endl;
            stop();
        }
    }

    boost::asio::io_context io_context_;
    boost::asio::ip::tcp::socket socket_;
    std::thread receive_thread_;
    std::atomic<bool> running_;
};

int main() {
    try {
        ChatClient client;
        
        // 서버 연결
        if (!client.connect("server", 8080)) {
            return 1;
        }

        std::cout << "채팅을 시작합니다. 종료하려면 /quit를 입력하세요." << std::endl;
        client.start();

    } catch (const std::exception& e) {
        std::cerr << "오류 발생: " << e.what() << std::endl;
        return 1;
    }

    return 0;
} 