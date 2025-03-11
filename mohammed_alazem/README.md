Proposal: Automated Travel and Visa Eligibility System Using API Integration

Objective

This project aims to develop an automated data pipeline that provides users with the fastest transportation option to an airport in Washington, D.C., while also verifying visa eligibility for their international destination. By integrating multiple data sources, this system ensures users can efficiently plan their journey and avoid unexpected travel restrictions.

Use Case Example

A user in downtown D.C. wants to fly to France. The system fetches Uber and metro travel times to Ronald Reagan Washington National Airport (DCA) and Dulles International Airport (IAD), recommends the fastest route, and confirms whether their passport allows visa-free entry to France. This allows the user to make an informed decision about their airport choice and travel eligibility before booking a ride.

Potential User Base

This system is useful for the following types of people. International travelers who need fast airport transport and visa verification,  business professionals who frequently travel abroad and tourists navigating Washington, D.C. for the first time.
Selected APIs and Justification

To achieve this goal, the system will leverage three external APIs:

Uber API – Fetches real-time ride estimates and ETAs from the user’s location to one of Washington, D.C.'s two metro-accessible airports, providing a private transport option.

Transport for Washington API – Retrieves real-time public transit data, enabling comparison with Uber to determine the fastest route to either DCA or IAD. 

Visa Free List API – Verifies whether the user's passport allows visa-free entry to their chosen destination or if a visa is required.

Links to APIs : 

https://publicapis.io/transport-for-washington-us-api

https://publicapis.io/uber-api

https://visafreelist.com

Conclusion

This project will demonstrate effective API integration, real-time data processing, and cloud storage solutions using Google Cloud Platform. The final system will be an automated travel assistant, ensuring users can reach the airport efficiently, confirm visa eligibility before departure, and plan trips seamlessly with real-time insights. This solution enhances travel efficiency, preparedness, and decision-making, reducing uncertainty and last-minute travel disruptions.
